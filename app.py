import inspect
import json
import logging
import os
import queue
import re
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import psycopg
import requests
from pymodbus.client import ModbusTcpClient

import stroohm_monta_api

LOG_LEVELS = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING,
              "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL}


def resolve_log_level(raw, default="WARNING"):
    """Map a LOG_LEVEL env value to a logging level, falling back to `default` on anything else.

    Returns (level_int, warning_message_or_None) rather than logging directly, since this runs
    before the logger below exists -- the module-level caller is responsible for printing the
    warning to stderr.
    """
    key = (raw or default).strip().upper()
    if key in LOG_LEVELS:
        return LOG_LEVELS[key], None
    return LOG_LEVELS[default], (
        f"Invalid LOG_LEVEL {key!r}; falling back to {default}. Valid values: {', '.join(LOG_LEVELS)}"
    )


# Resolved before any other env var, and without the env()/log helpers below, since the logger
# itself doesn't exist yet. Default WARNING for production: this hides the 5-second poll
# heartbeat and successful-push confirmations (all INFO), while every failure path in this file
# uses warning/error/exception and so always surfaces regardless of this setting.
_log_level, _log_level_warning = resolve_log_level(os.environ.get("LOG_LEVEL"))
if _log_level_warning:
    print(_log_level_warning, file=sys.stderr)

logging.basicConfig(level=_log_level, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
log = logging.getLogger("energy-monitor")


def env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        log.error("Missing required environment variable: %s", name)
        sys.exit(1)
    return value


def env_bool(name, default=False):
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


def env_int(name, default):
    raw = os.environ.get(name, default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        log.error("Environment variable %s must be an integer, got %r", name, raw)
        sys.exit(1)


# The solar side is an Anker SOLIX Solarbank (Max): a hybrid unit with its own MPPT inputs and a
# battery, read over Modbus TCP. The panels, the battery and the unit's AC output all come from it.
BATTERY_HOST = env("BATTERY_HOST", required=True)
BATTERY_PORT = env_int("BATTERY_PORT", 502)
# Unit id 1. Id 0 answers too; any other id gets no answer at all, so a wrong id shows up as a
# timeout, not as a Modbus exception.
BATTERY_UNIT_ID = env_int("BATTERY_UNIT_ID", 1)
SCAN_INTERVAL = env_int("SCAN_INTERVAL", 15)
BATTERY_TABLE = env("BATTERY_TABLE", "battery")
# Added to the unit's own lifetime PV counter (register 10018) to give total_power_yields. Set it to
# the previous inverter's final lifetime total when the unit replaces one, so the lifetime series --
# and everything that diffs it per day, PVOutput's lifetime figure included -- continues instead of
# restarting at 0. 0 for a system that started with this unit.
PV_TOTAL_OFFSET_KWH = float(env("PV_TOTAL_OFFSET_KWH", "0"))

POSTGRES_HOST = env("POSTGRES_HOST", required=True)
POSTGRES_PORT = env_int("POSTGRES_PORT", 5432)
POSTGRES_DB = env("POSTGRES_DB", "energy")
POSTGRES_USER = env("POSTGRES_USER", required=True)
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", required=True)
POSTGRES_TABLE = env("POSTGRES_TABLE", "solar")

# Not unconditionally required any more: MQTT is only needed if something actually uses it, which
# validate_config checks (publishing below, or plug ingestion further down). A deployment doing
# neither shouldn't have to invent a broker address.
MQTT_HOST = env("MQTT_HOST")
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_USERNAME = env("MQTT_USERNAME")
MQTT_PASSWORD = env("MQTT_PASSWORD")
MQTT_TOPIC_PREFIX = env("MQTT_TOPIC_PREFIX", "energy/solar").rstrip("/")
# Separate prefix for smart-meter electricity readings, so a subscriber can take one side without
# the other and neither has to filter by field name. Same ENABLE_MQTT_PUBLISH switch governs both;
# this one only produces anything when ENABLE_P1 is on, since that is where the readings come from.
MQTT_ELEC_TOPIC_PREFIX = env("MQTT_ELEC_TOPIC_PREFIX", "energy/electricity").rstrip("/")
# Re-publish each solar reading to MQTT, one retained topic per field under MQTT_TOPIC_PREFIX.
# Purely an outbound copy for other systems to consume -- nothing in this container reads it back,
# and Postgres is written either way, so turning it off loses no data here.
#
# Default OFF, matching the other outbound integrations here (ENABLE_PVOUTPUT, ENABLE_MINDERGAS):
# anything that leaves this container and lands somewhere else should be opted into, not inherited.
# This was on by default until 2026-08-14 -- **if you are upgrading and something subscribes to
# these topics, set ENABLE_MQTT_PUBLISH=true or it will go quiet.**
#
# The reason for the flip: publishing to topics nobody reads is invisible waste, and it is easy not
# to notice. On the deployment this was written for, all ~15 fields were being published every
# SCAN_INTERVAL while every consumer on the network had been retired or was still bound to a
# decommissioned exporter's topic. Off by default makes that state the explicit choice rather than
# the silent one.
#
# Gates *publishing* only -- plug ingestion (ENABLE_PLUGS) uses the same broker connection and is
# unaffected.
ENABLE_MQTT_PUBLISH = env_bool("ENABLE_MQTT_PUBLISH", False)
# How long to allow for the broker connection before warning, then re-warning, about it.
MQTT_WARN_AFTER = env_int("MQTT_WARN_AFTER", 60)
# Comma-separated register names to publish (e.g. "total_active_power,daily_power_yields"). Empty/unset
# publishes everything.
MQTT_PUBLISH_FIELDS = {f.strip() for f in env("MQTT_PUBLISH_FIELDS", "").split(",") if f.strip()}

# Smart-meter data (electricity + gas) is read directly off the P1 telegram stream, not via
# DSMR-reader's MQTT export or database -- DSMR-reader itself can keep running independently (own
# DB/admin UI) but is no longer a dependency for this container's own ingestion. P1_HOST/P1_PORT
# point at a plain-TCP relay of the meter's P1 port (e.g. ser2net).
#
# IMPORTANT: this container must be the *only* direct client of that relay/ser2net. Two separate
# ser2net `connection:` blocks pointed at the same serial device do NOT give two clients real
# concurrent access, despite briefly appearing to in testing -- ser2net only lets one connection
# actually hold the device open at a time. A short-lived test connection can coexist with an
# already-idle-stable one without tripping this, which is what made it look safe; the failure only
# shows up when one side needs to (re)open the device while the other is already holding it, which
# a long-lived second client (this container) guarantees will eventually happen. When it did, our
# connection (established once, never dropped) permanently starved DSMR-reader's periodic
# reconnects, which crash-looped (CPU spike) and lost telegram capture entirely until ENABLE_P1 was
# turned back off. If DSMR-reader (or anything else) also needs the P1 stream, point it at this
# container's own relay instead (ENABLE_P1_RELAY below) -- never at a second ser2net connection
# block on the same serial device.
#
# Raw telegrams carry no day-totals (DSMR-reader computes those from its own history) and no gas
# flow-rate, so there's no direct equivalent for those two fields -- a deliberate, documented gap:
# nothing currently reads electricity_day_totals live (see the stack README), and gas flow-rate
# was never promoted to a fixed column either.
ENABLE_P1 = env_bool("ENABLE_P1", False)
P1_HOST = env("P1_HOST")
P1_PORT = env_int("P1_PORT", 2001)
# Alternative upstream: an HTTP endpoint that returns the meter's latest raw telegram, as P1 dongles
# with a local API do (HomeWizard's P1 meter: http://<dongle>/api/v1/telegram). When set it replaces
# P1_HOST/P1_PORT: the dongle then holds the meter's P1 port, so there is no stream to connect to.
# The telegram arrives byte for byte, CRC included, and goes through the same extract_telegram/
# _handle_p1_telegram path and the same relay as a streamed one.
P1_URL = env("P1_URL")
# Seconds between polls of P1_URL. A DSMR 5 meter sends a telegram every second, and an unchanged
# response (same telegram) is skipped, so polling at the meter's own rate loses nothing.
P1_POLL_INTERVAL = env_int("P1_POLL_INTERVAL", 1)
ELEC_TABLE = env("ELEC_TABLE", "electricity")
GAS_TABLE = env("GAS_TABLE", "gas_positions")
# The `source` column value written for smart-meter rows. Still "dsmr" even though nothing goes
# through DSMR-reader any more, and deliberately not configurable: `source` identifies the *meter*,
# not the program that read it.
#
# Changing it splits history. It is a GROUP BY key in every electricity/gas continuous aggregate in
# the deploying stack (see its 002_continuous_aggregates.sql) and a filter in its dashboards, so a
# new value starts a parallel series at the cutover instant rather than continuing the existing one.
# Briefly writing "p1" here did exactly that: the day of the switch materialised as two rows per
# aggregate, panels joining solar_daily to electricity_daily matched both and double-counted that
# day's solar yield, the monthly rollups rendered the month as two split bars, and the data-gap
# monitoring panel -- which filters on this column -- went blind to the new series while flagging
# the switch itself as an outage. Keep it stable across ingestion-path changes.
P1_SOURCE = "dsmr"
# How long to wait for a telegram before warning the connection looks stalled. DSMR telegrams
# arrive roughly once a second, so anything on the order of a minute with nothing at all already
# means something's wrong, not sensor jitter.
P1_WARN_AFTER = env_int("P1_WARN_AFTER", 60)
# How long total silence may last before the connection is considered dead and rebuilt, however
# healthy the socket still looks. See p1_reader_loop for why a warning alone was not enough. Set
# comfortably above P1_WARN_AFTER so a stall is reported before it is acted on, and well above any
# plausible gap between telegrams -- at ~1/s, three minutes of nothing is not jitter. 0 disables
# the reconnect and restores the old warn-and-wait-forever behaviour; don't.
P1_STALL_TIMEOUT = env_int("P1_STALL_TIMEOUT", 180) or float("inf")
# Upper bound on the partial-telegram buffer extract_telegram may retain. Only ever consulted when
# a telegram start has been seen and no valid terminator follows it, so it costs nothing in normal
# operation: a real telegram off this meter is 723 bytes and the reader drains every *complete* one
# before anything is retained, leaving only a trailing fragment. 64 KB is therefore ~90 telegrams of
# slack -- reaching it means roughly a minute and a half of a stream that starts telegrams and never
# finishes them (ser2net at the wrong baud, or the port repointed at a non-DSMR device), which
# without a cap grows without limit in a container that has no mem_limit. Not env-configurable on
# purpose: there is no deployment where tuning this is the right answer.
P1_MAX_BUFFER_BYTES = 65536
# Re-serves the raw P1 byte stream to any number of downstream TCP clients (e.g. DSMR-reader),
# making this container the one and only real client of the upstream relay/ser2net -- see the
# ENABLE_P1 comment above for why that matters. Each connected client gets every byte this
# container itself reads, from the moment it connects onward (no replay of what it missed, same as
# connecting to ser2net directly). A client that stops reading gets a bounded send timeout rather
# than being allowed to block this container's own ingestion indefinitely.
ENABLE_P1_RELAY = env_bool("ENABLE_P1_RELAY", False)
P1_RELAY_BIND = env("P1_RELAY_BIND", "0.0.0.0")
P1_RELAY_PORT = env_int("P1_RELAY_PORT", 2000)
# How long a send to a downstream relay client may block before that client is dropped as dead.
P1_RELAY_CLIENT_TIMEOUT = env_int("P1_RELAY_CLIENT_TIMEOUT", 5)
# Backoff for the relay's own listening socket (bind and accept), not for its clients. Fixed rather
# than exponential: everything it retries past is a transient the operator resolves (a port still
# held by an outgoing container, exhausted fds), so retrying steadily and logging each attempt is
# more useful than backing off into silence.
RELAY_RETRY_INTERVAL = 5
# Minimum spacing between Postgres writes of electricity data. Telegrams arrive roughly once a
# second, finer than a dashboard needs; the in-memory cache (get_elec_values) still updates on
# every telegram regardless, so PVOutput and power_flow always see the latest reading even though
# what's stored is throttled. Matched to SCAN_INTERVAL by default so solar and electricity series
# share a resolution. 0 disables throttling. Gas isn't covered by this: it only changes every ~5
# minutes and is deduped on that basis already (see _handle_p1_telegram).
P1_MIN_INTERVAL = env_int("P1_MIN_INTERVAL", SCAN_INTERVAL)
# Cached electricity readings older than this are treated as absent, so a stalled P1 connection
# can't feed stale grid/house figures into a PVOutput upload or power_flow.
P1_MAX_DATA_AGE = env_int("P1_MAX_DATA_AGE", 300)

# Zigbee2MQTT smart plugs, an unrelated subsystem: submetering a handful of individual circuits
# rather than the whole house. These have no fixed schema across devices -- two common plug
# models (Aqara lumi.plug.mmeu01, Tuya TS0121) each publish extra device-specific fields (auto_off
# / led_disabled_night / power_outage_count on the Aqara, indicator_mode on the Tuya) alongside a
# common subset. Only that common subset (PLUG_FIELDS below, plus state) is kept, so a plug being
# swapped for a different model needs no code change.
ENABLE_PLUGS = env_bool("ENABLE_PLUGS", False)
# "topic=label,topic=label", e.g. "zigbee2mqtt/Fridge plug=fridge". The label becomes the
# `source` column value, so keep it short and stable even if the zigbee2mqtt friendly name changes.
PLUG_TOPICS = env("PLUG_TOPICS", "")
PLUG_TABLE = env("PLUG_TABLE", "smart_plugs")
PLUG_FIELDS = ("power", "energy", "current", "voltage")
# These plugs report on every attribute change, multiple times a minute -- finer than a dashboard
# needs. 0 disables throttling.
PLUG_MIN_INTERVAL = env_int("PLUG_MIN_INTERVAL", 5)

# Solar, grid and house power written together as one table, from values held in memory at the
# same instant -- a live as-of join across independently-polled solar/P1 readings would be more
# complex and costly than continuing to compute this once, at ingest. Written on *either* side
# updating (a fresh solar poll, or a fresh P1 telegram), each time using the other side's latest
# cached value, so the P1 side keeps house/grid moving at telegram cadence rather than only once
# per SCAN_INTERVAL.
ENABLE_DERIVED = env_bool("ENABLE_DERIVED", True)
DERIVED_TABLE = env("DERIVED_TABLE", "power_flow")
# How long a battery poll stays "current" for power_flow purposes -- comfortably above
# SCAN_INTERVAL so a single missed poll doesn't trip it. Past it, the solar and battery terms are
# unknown and power_flow omits them; see _battery_values_for_derived.
SOLAR_STALE_AFTER = env_int("SOLAR_STALE_AFTER", 120)


def parse_plug_topics(raw):
    """'topic=label,topic=label' -> {topic: label}.

    Malformed entries are skipped with a warning rather than raised, so one typo doesn't take
    down every other configured plug.
    """
    topics = {}
    for entry in (e.strip() for e in raw.split(",")):
        if not entry:
            continue
        topic, sep, label = entry.partition("=")
        topic, label = topic.strip(), label.strip()
        if not sep or not topic or not label:
            log.warning("Ignoring malformed PLUG_TOPICS entry %r (expected 'topic=label')", entry)
            continue
        topics[topic] = label
    return topics


PLUG_TOPIC_MAP = parse_plug_topics(PLUG_TOPICS)

# Whether a broker connection is needed at all. Both directions share one client, so this is true if
# either uses it; when neither does, main() skips connecting and skips the watchdog rather than
# maintaining a connection nothing sends or receives on (and warning every MQTT_WARN_AFTER about a
# broker that isn't wanted).
MQTT_ENABLED = ENABLE_MQTT_PUBLISH or ENABLE_PLUGS

ENABLE_PVOUTPUT = env_bool("ENABLE_PVOUTPUT", False)
PVOUTPUT_API_KEY = env("PVOUTPUT_API_KEY")
PVOUTPUT_SYSTEM_ID = env("PVOUTPUT_SYSTEM_ID")
PVOUTPUT_INTERVAL = env_int("PVOUTPUT_INTERVAL", 300)
# PVOutput's c1 flag: 1 = both v1 and v3 are lifetime energy values, 2 = only v1 is, 3 = only v3
# is. Omitting it (or 0) means v1/v3 are taken as-is, i.e. day totals.
#
# Default 0: the default v1 mapping (daily_generation_wh) is a day total that resets to 0 at
# midnight, not a true lifetime counter, so c1 must stay unset for it. The old SunGather config
# shipped cumulative_flag: 2 for this same mapping, which looks like it should still work -- the
# within-day delta from a counter starting at 0 each day equals that day's generation -- but
# PVOutput's own baseline tracking for c1 is documented as unreliable for values that reset daily
# (https://forum.pvoutput.org/t/c1-1-first-positive-value-is-subtracted-from-all-subsequent-values/8930),
# and it left Energy Generation stuck at 0 in production despite Power/Voltage reporting fine. If
# you remap v1/v3 to a genuine lifetime metric (e.g. total_generation_wh), set this to match.
PVOUTPUT_CUMULATIVE_FLAG = env_int("PVOUTPUT_CUMULATIVE_FLAG", 0)
# Don't upload a reading older than this -- otherwise a stalled Modbus connection would keep
# re-posting the last known value stamped with the current time.
PVOUTPUT_MAX_DATA_AGE = env_int("PVOUTPUT_MAX_DATA_AGE", 600)


ENABLE_MINDERGAS = env_bool("ENABLE_MINDERGAS", False)
MINDERGAS_API_KEY = env("MINDERGAS_API_KEY")
MINDERGAS_API_URL = "https://www.mindergas.nl/api/meter_readings"
MINDERGAS_HOUR = env_int("MINDERGAS_HOUR", 0)
MINDERGAS_MINUTE = env_int("MINDERGAS_MINUTE", 5)
MINDERGAS_RETRY_INTERVAL = env_int("MINDERGAS_RETRY_INTERVAL", 900)
# Where to read the gas meter position back from. Defaults line up with what p1_reader_loop
# writes (see GAS_TABLE), so they only need changing if that's customised. This always reads from
# Postgres, not any in-memory cache: Mindergas wants the last reading *before local midnight*,
# which needs timestamped history, not the current value.
MINDERGAS_GAS_TABLE = env("MINDERGAS_GAS_TABLE", GAS_TABLE)
MINDERGAS_GAS_FIELD = env("MINDERGAS_GAS_FIELD", "delivered")

# Monta (EV charge card) sessions, fetched from Monta's Public API by stroohm_monta_api.py
# (github.com/puijken/stroohm-monta-api, added at image build) and upserted
# into MONTA_TABLE. Not energy *measured* here -- it's charging done elsewhere, on public and work
# chargers -- but the same daily-integration shape as Mindergas, and it lands in the same database
# as the home charging it's compared against. Every run re-fetches the whole history (or the last
# MONTA_LOOKBACK_DAYS), so a session corrected after the fact is corrected here too; the upsert on
# Monta's session id makes that idempotent. See README "Monta charging sessions".
ENABLE_MONTA = env_bool("ENABLE_MONTA", False)
MONTA_CLIENT_ID = env("MONTA_CLIENT_ID")
MONTA_CLIENT_SECRET = env("MONTA_CLIENT_SECRET")


def parse_monta_times(raw):
    """'21:00,04:00' -> [(4, 0), (21, 0)] (sorted, deduplicated), or None if anything is malformed."""
    try:
        times = set()
        for part in (raw or "").split(","):
            hh, mm = part.strip().split(":")
            h, m = int(hh), int(mm)
            if not (0 <= h < 24 and 0 <= m < 60):
                return None
            times.add((h, m))
        return sorted(times) or None
    except ValueError:
        return None


# Local times of day to fetch, comma-separated. Twice a day by default: an evening run so a
# session that ended in the early evening is on the dashboards the same night, and an early
# morning one for anything that finished later.
MONTA_TIMES_RAW = env("MONTA_TIMES", "04:00,21:00")
MONTA_TIMES = parse_monta_times(MONTA_TIMES_RAW)
MONTA_RETRY_INTERVAL = env_int("MONTA_RETRY_INTERVAL", 900)
MONTA_LOOKBACK_DAYS = env_int("MONTA_LOOKBACK_DAYS", 0)   # 0 = everything, every run
MONTA_TABLE = env("MONTA_TABLE", "monta_charges")
MONTA_VAT_RATE = float(env("MONTA_VAT_RATE", str(stroohm_monta_api.DEFAULT_VAT_RATE)))
MONTA_COLUMNS = ["id", "human_readable_id", "state", "charge_point_id", "started_at", "stopped_at",
                 "energy_kwh", "price_incl_vat", "price_excl_vat", "currency", "raw"]

# The Solarbank's Modbus map. Documented registers come from the config in Anker's own Home
# Assistant integration (anker-charging/ha-anker-solix-official) for this model; the rest were
# identified against the device and are marked as such. Facts the reads rely on, all verified:
#   - 32-bit values are big-endian, HIGH word first (the reverse of many inverters).
#   - Addresses are wire addresses; there is no 1-based offset.
#   - A read must start on the first word of a value; asking for the second word alone returns
#     exception 2, the same as an unmapped address.
#   - Inside a block read the firmware returns 0 for some undocumented registers (10254, 10156),
#     so those are read on their own.
#   - Function codes 3 and 4 return the same data.
# This code only ever reads. Register 10064 is the operating mode, 10071 the power setpoint and
# 60000-60003 the charge limits, all writable: nothing here may write to them.
BLOCK_A = (10000, 51)   # status, powers, SOC, PV total, power limits
BLOCK_B = (10199, 67)   # AC side, AC output, charge/discharge totals
MODE_REGISTER = 10064   # holding register
CELL_POWER_REGISTER = 10254
TEMPERATURE_REGISTER = 10156
SOC_LIMIT_REGISTERS = (60000, 4)
# The two MPPT (string) inputs, undocumented: PV1 voltage (0.1 V), PV1 current (0.01 A), then the
# same for PV2, one word each. Identified on a sunny morning: voltage x current of both strings
# added up to the PV power in 10002 within about 1 %. A block read of these four matches single
# reads; the neighbouring addresses are unmapped, so the block cannot be widened.
MPPT_REGISTERS = (10167, 4)

BATTERY_STATUS = {0: "standby", 1: "charging", 2: "discharging", 3: "sleep"}
OPERATING_MODES = {0: "self_consumption", 1: "tou_mode", 3: "third_party_control", 4: "custom_mode",
                   5: "socket_overlay_mode", 6: "smart_mode", 7: "dynamic_pricing"}

# The `source` value of the PV rows in POSTGRES_TABLE. "sungrow" although the PV now comes through
# the battery, for the same reason P1_SOURCE stays "dsmr": it is a GROUP BY key in the solar
# continuous aggregates and the filter of the daily-yield view, which diffs each day's highest
# lifetime total against the previous day's. A new value would start a second series at the switch
# and break that diff; with PV_TOTAL_OFFSET_KWH continuing the lifetime total, keeping the label
# continues the series instead.
SOLAR_SOURCE = "sungrow"
BATTERY_SOURCE = "anker"

# What poll_battery() returns as the solar values, which is what MQTT_PUBLISH_FIELDS is validated
# against. daily_power_yields is added by the poll loop (see daily_yield_kwh) and can be absent, and
# the string fields are absent when their (optional) read fails.
PUBLISHABLE_FIELDS = {"total_active_power", "total_power_yields", "total_power_yields_precise",
                      "daily_power_yields", "mppt_1_voltage", "mppt_1_current", "mppt_2_voltage",
                      "mppt_2_current", "total_dc_power"}

# Fixed columns of the battery table; everything else poll_battery() returns lands in its `extra`.
BATTERY_COLUMNS = {
    "soc", "battery_power", "pv_power", "third_party_pv_power", "load_power", "grid_power",
    "ac_output_power", "charge_energy_total", "discharge_energy_total", "pv_energy_total", "status",
    "operating_mode",
}

TABLE_COLUMNS = {
    # The string columns were the previous inverter's and are filled again from the Solarbank's
    # MPPT registers. The rest (temperature, running time, AC side) stay NULL: no equivalent.
    POSTGRES_TABLE: {"daily_power_yields", "total_power_yields", "total_active_power",
                     "mppt_1_voltage", "mppt_1_current", "mppt_2_voltage", "mppt_2_current",
                     "total_dc_power"},
    BATTERY_TABLE: BATTERY_COLUMNS,
    # Must stay in step with the deploying stack's schema: a name listed here is written as a real
    # column, so adding one without the matching ALTER TABLE makes every insert fail. Apply the
    # schema change *before* deploying an image that adds a field here -- old code ignores a column
    # it doesn't know about, so that order is safe in a way the reverse isn't.
    ELEC_TABLE: {
        "electricity_delivered_1", "electricity_returned_1", "electricity_delivered_2",
        "electricity_returned_2", "electricity_currently_delivered",
        "electricity_currently_returned", "phase_voltage_l1",
        "electricity_tariff", "phase_power_current_l1",
    },
    GAS_TABLE: {"delivered"},
    PLUG_TABLE: set(PLUG_FIELDS) | {"state"},
    DERIVED_TABLE: {"solar_w", "grid_w", "house_w"},
}


def _e(elec, field):
    try:
        return float(elec[field])
    except (KeyError, TypeError, ValueError) as exc:
        raise AssertionError(f"P1 electricity field {field!r} missing or non-numeric") from exc


# Metrics that can be mapped onto PVOutput parameters, each already normalised to the unit
# PVOutput expects (Wh for energy, W for power) so the mapping never needs scaling syntax.
#
# Each entry is (needs_electricity, function(inverter_values, electricity_values)). Metrics that
# don't need electricity come straight from the Modbus poll and always work; the rest read the
# cached P1 reading (see get_elec_values), so no Postgres round-trip is involved and they stay
# usable even if writes are failing. `e` is None (never accessed) when the metric doesn't need it.
METRICS = {
    # --- from the battery poll (always available) ---
    # daily_power_yields is the lifetime total minus the last total before local midnight, computed
    # in the poll loop (daily_yield_kwh). Absent when there is no baseline, which omits v1.
    "daily_generation_wh": (False, lambda i, e: round(i["daily_power_yields"] * 1000)),
    "total_generation_wh": (False, lambda i, e: round(i["total_power_yields"] * 1000)),
    "generation_w": (False, lambda i, e: i["total_active_power"]),
    "battery_soc": (False, lambda i, e: i["soc"]),
    # --- from the smart meter (grid side) ---
    "grid_import_w": (True, lambda i, e: round(_e(e, "electricity_currently_delivered") * 1000)),
    "grid_export_w": (True, lambda i, e: round(_e(e, "electricity_currently_returned") * 1000)),
    "grid_voltage_v": (True, lambda i, e: _e(e, "phase_voltage_l1")),
    "total_import_wh": (
        True, lambda i, e: round((_e(e, "electricity_delivered_1") + _e(e, "electricity_delivered_2")) * 1000)
    ),
    "total_export_wh": (
        True, lambda i, e: round((_e(e, "electricity_returned_1") + _e(e, "electricity_returned_2")) * 1000)
    ),
    # --- derived: what the house actually used (grid + the battery unit's AC output) ---
    # Not generation + grid any more: with a battery between the panels and the house, part of
    # the generation goes into the cells and comes out later. The unit's AC output (register 10208,
    # positive into the house) is what it actually delivers, whatever mix of panels and cells.
    "house_power_w": (
        True,
        lambda i, e: round(
            i["ac_output_power"]
            + (_e(e, "electricity_currently_delivered") - _e(e, "electricity_currently_returned")) * 1000
        ),
    ),
    # No lifetime house energy: it would need a lifetime counter of the unit's AC output, and none
    # has been identified (10264 counts at the cells and includes standby drain).
    # No daily_import_wh/daily_export_wh/daily_house_energy_wh equivalent: raw telegrams carry no
    # day-totals (see the comment above ENABLE_P1), so there's nothing to compute these from.
}

# Metrics that are lifetime counters rather than day totals. PVOutput's c1 flag has to agree with
# whichever of these lands on v1/v3, or the uploaded energy figures come out wrong.
LIFETIME_METRICS = {"total_generation_wh", "total_import_wh", "total_export_wh"}

# PVOUTPUT_V1..V6 name a metric each; unset means that parameter is not uploaded. The defaults
# reproduce exactly what the previous SunGather setup sent.
PVOUTPUT_MAPPING = {
    param: name
    for param, name in (
        (param, env(f"PVOUTPUT_{param.upper()}", default))
        for param, default in (("v1", "daily_generation_wh"), ("v2", "generation_w"), ("v3", ""),
                               ("v4", ""), ("v5", ""), ("v6", ""))
    )
    if name
}

_state_lock = threading.Lock()
latest_values = {}
latest_poll_monotonic = None

# The paho client, published to from two threads: the poll loop (inverter readings) and
# p1_reader_loop (smart-meter readings). Held as module state rather than threaded through because
# p1_reader_loop is started without it; paho's publish() is thread-safe, so no lock is needed. None
# when MQTT is disabled entirely (see MQTT_ENABLED) or before main() has run.
_mqtt_client = None

# Latest electricity reading from the P1 telegram stream, as (values, monotonic_time) -- read by
# build_pvoutput_params/derived_fields, written only by p1_reader_loop's own thread.
_elec_lock = threading.Lock()
_elec_cache = None

# Today's local-midnight total_power_yields baseline, as (local_date, baseline_kwh), read/written
# by daily_yield_kwh() from the poll loop. Refreshed lazily -- the cached date just has to stop
# matching "today" for a new query to fire -- rather than on a timer, so a missed refresh at exactly
# midnight self-heals on the next poll. _daily_yield_retry_after holds off a failed lookup so a
# database outage costs one query a minute, not one per poll.
_daily_yield_lock = threading.Lock()
_daily_yield_baseline = None
_daily_yield_retry_after = 0.0
# Monotonic time of the last electricity Postgres write, for P1_MIN_INTERVAL throttling. Owned
# solely by p1_reader_loop's thread -- no lock needed.
_last_elec_write_time = 0.0

# Last (timestamp, value) gas reading written, so a telegram repeating the same still-current
# M-Bus capture (every ~1s, between the meter's own ~5-minute gas updates) doesn't get rewritten.
# Postgres would collapse it anyway (ON CONFLICT on the same time+source), but there's no point
# spending the write. Owned solely by p1_reader_loop -- no lock needed.
_last_gas_written = None

# Monotonic time of the last Postgres write per plug label, for PLUG_MIN_INTERVAL throttling.
_plug_last_write_time = {}
# Labels already logged once at INFO on first message, so ongoing traffic doesn't repeat it.
_plug_seen = set()

# Every Postgres write (solar poll, plug MQTT message, P1 gas reading) is handed to a worker
# thread rather than performed inline. A blocking write inside the MQTT callback in particular
# stops the socket being drained and the broker discards messages for a slow consumer -- measured
# as 6 of 20 lost with a synchronous write (against the old HTTP-based InfluxDB write path, but the
# same physical constraint applies to any blocking write here). During a Postgres outage the
# writer holds its current point and retries (see _write_until_done), so this queue is what
# buffers the outage. Bounded so a prolonged one cannot grow without limit; oldest points are
# dropped first, since fresh readings matter more than stale ones.
_write_queue = queue.Queue(maxsize=env_int("WRITE_QUEUE_SIZE", 2000))


def _u32(words, base, address):
    """Big-endian 32-bit value at `address` in a block read starting at `base`."""
    i = address - base
    return (words[i] << 16) | words[i + 1]


def _i32(words, base, address):
    value = _u32(words, base, address)
    return value - 0x100000000 if value & 0x80000000 else value


# pymodbus renamed this argument from `slave` to `device_id` (3.14 dropped `slave` outright), so
# resolve it from the signature instead of hardcoding either name. Dependabot auto-merges anything
# it doesn't classify as semver-major, which has already silently moved this pin once.
_UNIT_KWARG = (
    "device_id"
    if "device_id" in inspect.signature(ModbusTcpClient.read_input_registers).parameters
    else "slave"
)


def _read(client, function, address, count):
    """One read, function 3 (holding) or 4 (input). Never anything else: this code only reads."""
    reader = {3: client.read_holding_registers, 4: client.read_input_registers}[function]
    result = reader(address, count=count, **{_UNIT_KWARG: BATTERY_UNIT_ID})
    if result.isError():
        raise OSError(f"Modbus read of {address} failed: {result}")
    return result.registers


def _read_optional(client, function, address, count):
    """A read whose failure costs only its own fields, not the poll."""
    try:
        return _read(client, function, address, count)
    except Exception:
        log.debug("Optional read of %s failed", address, exc_info=True)
        return None


def poll_battery(client):
    """One poll. Returns (solar, battery): the PV values for POSTGRES_TABLE and the battery row.

    The battery row keeps the field names the dashboards read; names not in BATTERY_COLUMNS land in
    its `extra`. The undocumented registers are named after what they were shown to do; see the
    comments.
    """
    a_base, a_count = BLOCK_A
    b_base, b_count = BLOCK_B
    a = _read(client, 4, a_base, a_count)
    b = _read(client, 4, b_base, b_count)
    mode = _read(client, 3, MODE_REGISTER, 1)[0]

    soc = a[10014 - a_base]
    if soc > 100:
        raise ValueError(f"implausible state of charge {soc}")
    status = a[10001 - a_base]
    pv_power = _i32(a, a_base, 10002)
    third_party_pv_power = _i32(a, a_base, 10004)
    pv_energy_total = _u32(a, a_base, 10018) / 10

    battery = {
        "soc": soc,
        # Positive = discharging, negative = charging (the device's own sign).
        "battery_power": _i32(a, a_base, 10008),
        "pv_power": pv_power,
        "third_party_pv_power": third_party_pv_power,
        # The unit's view of load and grid, from its own CT meter. Only as good as that meter's
        # wiring; the P1 meter stays the source for grid figures.
        "load_power": _i32(a, a_base, 10010),
        "grid_power": _i32(a, a_base, 10012),
        # What the unit delivers into the house circuit, positive into the house.
        "ac_output_power": _i32(b, b_base, 10208),
        # Lifetime counters at the cells, 0.1 kWh. The discharge counter includes standby drain.
        "charge_energy_total": _u32(b, b_base, 10262) / 10,
        "discharge_energy_total": _u32(b, b_base, 10264) / 10,
        "pv_energy_total": pv_energy_total,
        "status": BATTERY_STATUS.get(status, str(status)),
        "operating_mode": OPERATING_MODES.get(mode, str(mode)),
        # --- extra: undocumented, named after what they were shown to do ---
        "soh": a[10015 - a_base],
        "ac_voltage": b[10199 - b_base] / 10,
        "ac_current": b[10205 - b_base] / 100,
        # Not real power: ~30 idle, 0-79 while 800 W flowed. Kept raw, probably reactive power.
        "ac_power_abs": _i32(b, b_base, 10210),
        "frequency": b[10213 - b_base] / 100,
        # Unknown; they step with the charging state. Kept raw until identified.
        "raw_10224": b[10224 - b_base],
        "raw_10227": b[10227 - b_base],
        # 0.1 kWh counters identified from their step rate at a steady charge: energy in at the
        # socket, and the unit's own consumption (conversion loss plus standby).
        "socket_energy_in": _u32(a, a_base, 10030) / 10,
        "own_use_energy": _u32(a, a_base, 10026) / 10,
        "max_charge_power": _i32(a, a_base, 10036),
        "max_discharge_power": _i32(a, a_base, 10038),
        "rated_energy": _u32(b, b_base, 10250) / 10,
    }
    cell = _read_optional(client, 4, CELL_POWER_REGISTER, 2)
    # Power into the cells, W, read on its own (a block read returns 0 for it).
    battery["ac_power"] = _i32(cell, CELL_POWER_REGISTER, CELL_POWER_REGISTER) if cell else None
    temperature = _read_optional(client, 4, TEMPERATURE_REGISTER, 1)
    # 0.1 degrees C; matched the app's reading. Which sensor is not known.
    battery["temperature"] = temperature[0] / 10 if temperature else None
    limits = _read_optional(client, 3, *SOC_LIMIT_REGISTERS)
    if limits:
        battery.update(charge_limit_soc=limits[0], discharge_limit_soc=limits[1],
                       backup_reserve_soc=limits[2], backup_reserve_enabled=limits[3])

    total = round(PV_TOTAL_OFFSET_KWH + pv_energy_total, 1)
    solar = {
        # Same meaning as before the battery: what the panels produce, W.
        "total_active_power": pv_power + third_party_pv_power,
        # 0.1 kWh resolution. The _precise twin keeps the name the daily baseline reads from extra.
        "total_power_yields": total,
        "total_power_yields_precise": total,
    }
    mppt = _read_optional(client, 4, *MPPT_REGISTERS)
    if mppt:
        v1, i1, v2, i2 = mppt[0] / 10, mppt[1] / 100, mppt[2] / 10, mppt[3] / 100
        solar.update(mppt_1_voltage=v1, mppt_1_current=i1, mppt_2_voltage=v2, mppt_2_current=i2,
                     # DC power of both strings, W. Tracks 10002 within about 1 %; a separate
                     # figure, not a check on it.
                     total_dc_power=round(v1 * i1 + v2 * i2))
    return solar, battery


_pg_conn = None


def get_pg_write_connection():
    """Returns the persistent connection used by the writer thread, (re)connecting on demand.

    A single connection reused by the one writer thread -- no pool needed, since nothing else
    ever writes concurrently (mirrors the single-persistent-HTTP-session shape of the old
    InfluxDB write path). Autocommit, since each write is already its own unit of work.
    """
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        _pg_conn = psycopg.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
            user=POSTGRES_USER, password=POSTGRES_PASSWORD, autocommit=True,
        )
    return _pg_conn


def write_postgres(values, ts_ns, table=None, source="sungrow"):
    """Writes one point to `table`, split into known fixed columns plus `extra` for anything
    not in TABLE_COLUMNS. ON CONFLICT DO UPDATE mirrors the old InfluxDB write path's semantics:
    writing the same (time, source) again overwrites rather than silently keeping the first
    write, which matters for the rare case of a corrected re-publish at the same measurement
    timestamp."""
    table = table or POSTGRES_TABLE
    known = TABLE_COLUMNS.get(table, set())
    direct = {k: v for k, v in values.items() if k in known}
    extra = {k: v for k, v in values.items() if k not in known}

    ts = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
    columns = ["time", "source", "extra"] + list(direct)
    params = [ts, source, json.dumps(extra) if extra else None] + list(direct.values())
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    update_cols = ["extra"] + list(direct)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)

    conn = get_pg_write_connection()
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT (time, source) DO UPDATE SET {set_clause}",
            params,
        )


def get_last_gas_reading(before):
    """Last (time, value) of MINDERGAS_GAS_FIELD in MINDERGAS_GAS_TABLE strictly before `before`,
    or None. No lower bound: Mindergas wants the actual final reading of the day, not merely
    "a recent-enough one", so this must keep looking back however far it takes rather than
    giving up past some fixed window. Opens a short-lived connection rather than sharing the
    writer thread's persistent one -- this runs at most once a day, from a different thread."""
    with psycopg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT time, "{MINDERGAS_GAS_FIELD}" FROM {MINDERGAS_GAS_TABLE} '
                f"WHERE time < %s ORDER BY time DESC LIMIT 1",
                (before,),
            )
            return cur.fetchone()


def get_daily_yield_baseline(before):
    """(total_power_yields, total_power_yields_precise-or-None) of the last SOLAR_SOURCE row
    in POSTGRES_TABLE strictly before `before` (a local midnight), or None if there isn't one.

    This is yesterday's final lifetime-yield reading, which is the same thing as today's starting
    point since total_power_yields is continuous across midnight -- see
    daily_yield_kwh() below, and the README's "Solar daily yield" section for why
    this reads the lifetime counter instead of trusting daily_power_yields (which periodically
    fails to reset). No lower bound, same reasoning as get_last_gas_reading: a genuine gap in our
    own polling right around midnight should still resolve against the last real reading, however
    far back it is, rather than giving up. `source` is pinned to SOLAR_SOURCE so a backfill/gapfill
    row (see backfill.py) is never picked as the baseline. Opens a short-lived connection rather
    than sharing the writer thread's persistent one -- this is refreshed at most once a day, from
    pvoutput_loop's thread."""
    with psycopg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT total_power_yields, (extra->>'total_power_yields_precise')::double precision "
                f"FROM {POSTGRES_TABLE} WHERE time < %s AND source = %s ORDER BY time DESC LIMIT 1",
                (before, SOLAR_SOURCE),
            )
            return cur.fetchone()


def daily_yield_kwh(total_kwh):
    """Today's generation: `total_kwh` minus the last lifetime total before local midnight, or None
    when there is no earlier row to start from (or the lookup is failing).

    Diffing the lifetime counter is how every daily figure in the stack is computed (see the README's
    "Solar daily yield" section), so the live value and the dashboards agree by construction. The
    baseline is cached per local day; see _daily_yield_baseline.
    """
    global _daily_yield_baseline, _daily_yield_retry_after
    now_local = datetime.now().astimezone()
    today = now_local.date()
    with _daily_yield_lock:
        cached = _daily_yield_baseline

    if cached is None or cached[0] != today:
        if time.monotonic() < _daily_yield_retry_after:
            return None
        local_midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            row = get_daily_yield_baseline(local_midnight)
        except Exception:
            log.exception("Could not fetch the daily-yield baseline; retrying in a minute")
            _daily_yield_retry_after = time.monotonic() + 60
            return None
        if row is None:
            log.warning("No solar row before %s to start today's yield from", local_midnight)
            _daily_yield_retry_after = time.monotonic() + 3600
            return None
        baseline_total, baseline_precise = row
        cached = (today, baseline_precise if baseline_precise is not None else baseline_total)
        with _daily_yield_lock:
            _daily_yield_baseline = cached

    # max(0, ...) guards boundary jitter rather than ever reporting negative generation.
    return round(max(0.0, total_kwh - cached[1]), 1)


def publish_mqtt(mqtt_client, values, prefix, fields=None):
    """Publishes each value as its own retained topic under `prefix`.

    `fields`, when given, is an allowlist of names to publish. Only the inverter side passes one
    (MQTT_PUBLISH_FIELDS, documented in terms of register names); the smart-meter side has a small
    fixed field set and no equivalent knob, so filtering it by register names would be meaningless.

    Retained so a subscriber connecting later immediately gets the current value instead of waiting
    for the next update -- which matters more on the meter side, where a field that stops changing
    would otherwise look absent.
    """
    for name, value in values.items():
        if fields and name not in fields:
            continue
        mqtt_client.publish(f"{prefix}/{name}", value, retain=True)


def build_pvoutput_params(values):
    """Resolves PVOUTPUT_MAPPING into upload parameters.

    Any parameter needing electricity data is dropped if that data is missing or stale, so a
    stalled P1 connection degrades this to a generation-only upload instead of losing it or
    reporting figures from hours ago.
    """
    elec = get_elec_values() if ENABLE_P1 else None
    needs_elec = {name for name in PVOUTPUT_MAPPING.values() if METRICS[name][0]}
    if needs_elec and elec is None:
        log.warning("No fresh electricity data from P1; omitting %s", ", ".join(sorted(needs_elec)))

    now = datetime.now()
    params = {"d": now.strftime("%Y%m%d"), "t": now.strftime("%H:%M"), "c1": PVOUTPUT_CUMULATIVE_FLAG}
    for param, name in sorted(PVOUTPUT_MAPPING.items()):
        needs, compute = METRICS[name]
        if needs and elec is None:
            continue
        try:
            params[param] = compute(values, elec)
        except Exception:
            log.exception("Could not compute %s for %s; omitting it", name, param)
    return params


def push_pvoutput(values):
    params = build_pvoutput_params(values)
    if not {"v1", "v2"} & params.keys():
        log.error("PVOutput needs at least v1 or v2; nothing uploadable in %s", params)
        return
    if not ENABLE_PVOUTPUT:
        log.info("PVOutput push skipped (ENABLE_PVOUTPUT=false), would send: %s", params)
        return
    resp = requests.post(
        "https://pvoutput.org/service/r2/addstatus.jsp",
        headers={"X-Pvoutput-Apikey": PVOUTPUT_API_KEY, "X-Pvoutput-SystemId": PVOUTPUT_SYSTEM_ID},
        data=params,
        timeout=10,
    )
    # PVOutput explains rejections in the body ("Bad request 400: Moon powered!" etc.), which
    # raise_for_status() would discard.
    if resp.status_code != 200:
        raise AssertionError(f"PVOutput rejected the upload: {resp.status_code} {resp.text}")
    log.info("Pushed to PVOutput: %s", params)


def push_mindergas():
    # Mirrors DSMR-reader's own exporter (src/dsmr_mindergas/services.py): take the last gas
    # reading strictly before local midnight -- i.e. the previous day's actual final reading --
    # and POST {"date": <that day>, "reading": "<m3>"}. The value is read back out of the
    # gas_positions table p1_reader_loop writes.
    #
    # Timestamps must be timezone-aware: Postgres stores timestamptz as UTC and reads a naive
    # value as the session's local time, so passing naive local time here would be ambiguous.
    now_local = datetime.now().astimezone()
    midnight = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    row = get_last_gas_reading(midnight)
    if row is None:
        raise AssertionError(
            f"No gas reading found before {midnight.isoformat()} "
            f"-- is ENABLE_P1 on and is the P1 relay reachable?"
        )

    _reading_time, reading = row
    payload = {"date": (midnight - timedelta(days=1)).date().isoformat(), "reading": str(reading)}

    if not ENABLE_MINDERGAS:
        log.info("Mindergas push skipped (ENABLE_MINDERGAS=false), would send: %s", payload)
        return

    resp = requests.post(
        MINDERGAS_API_URL,
        headers={"Content-Type": "application/json", "AUTH-TOKEN": MINDERGAS_API_KEY},
        data=json.dumps(payload),
        timeout=10,
    )
    if resp.status_code != 201:
        raise AssertionError(f"Unexpected status code from Mindergas: {resp.status_code} {resp.text}")
    log.info("Pushed to Mindergas: %s", payload)


def _on_mqtt_message(client, userdata, message):
    plug_label = PLUG_TOPIC_MAP.get(message.topic)
    if plug_label is not None:
        _handle_plug_message(message, plug_label)


def crc16_arc(data):
    """CRC-16/ARC (poly 0xA001, init 0, no final XOR) -- what DSMR telegrams are checksummed
    with. A torn or corrupted read must not silently reach Postgres as a plausible-looking gas
    reading, so every telegram is verified against this before its contents are trusted."""
    crc = 0
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc & 0xFFFF


# Matches the telegram's final line ("!" + 4 hex CRC digits + CRLF) -- the only place "!" appears
# in a well-formed telegram, so a plain search for it is unambiguous.
_TELEGRAM_END_RE = re.compile(rb"!([0-9A-Fa-f]{4})\r\n")


def extract_telegram(buffer):
    """Looks for one complete, CRC-verified telegram at the start of `buffer`.

    Returns (telegram_text, remaining_buffer). telegram_text is None if no complete telegram is
    available yet, or if one was found but failed its CRC check (logged and discarded either way
    -- the remaining buffer still advances past it, so a single bad telegram can't wedge the
    reader waiting for a checksum that already failed). Bytes before the first "/" are dropped: a
    freshly opened connection can land mid-telegram, and there's nothing useful to keep before a
    telegram start.
    """
    start = buffer.find(b"/")
    if start == -1:
        return None, b""
    buffer = buffer[start:]

    match = _TELEGRAM_END_RE.search(buffer)
    if not match:
        if len(buffer) > P1_MAX_BUFFER_BYTES:
            # Everything here is unparseable by definition -- a telegram start with no terminator
            # for P1_MAX_BUFFER_BYTES. Dropping all of it rather than keeping a tail from the last
            # "/" is deliberate: on a garbage stream "/" occurs about every 256 bytes, so a tail
            # would just refill, and on a stream that recovers the next telegram resyncs within a
            # second anyway. Worst case is losing one in-flight telegram at the boundary, in a
            # situation that is already producing no data. The relay is unaffected -- it broadcasts
            # each raw chunk before this buffer is ever touched.
            log.warning(
                "Discarding %d bytes of P1 data: telegram start seen but no valid terminator "
                "within %d bytes. Check the upstream is really a DSMR meter at the right baud.",
                len(buffer), P1_MAX_BUFFER_BYTES,
            )
            return None, b""
        return None, buffer

    telegram = buffer[: match.end()]
    remaining = buffer[match.end() :]

    # The CRC covers everything from "/" through and including "!", excluding the 4 hex digits
    # and the trailing CRLF.
    computed = crc16_arc(telegram[: match.start() + 1])
    declared = int(match.group(1), 16)
    if computed != declared:
        log.warning("P1 telegram CRC mismatch (declared %04X, computed %04X); discarding", declared, computed)
        return None, remaining

    return telegram.decode("ascii", errors="replace"), remaining


_OBIS_LINE_RE = re.compile(r"^(\d+-\d+:\d+\.\d+\.\d+)((?:\([^)]*\))+)$")
_OBIS_GROUP_RE = re.compile(r"\(([^)]*)\)")
_DSMR_TIMESTAMP_RE = re.compile(r"^(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})([SW])$")

# OBIS code -> the electricity column it fills, confirmed against a real captured telegram
# (standard Dutch DSMR 5.0, single-phase -- no L2/L3 codes to map). Matches DSMR-MQTT.md's field
# names exactly, so the Postgres schema/derived_fields/PVOutput METRICS need no changes at all,
# only the ingestion source does.
_ELEC_OBIS_MAP = {
    "1-0:1.8.1": "electricity_delivered_1",
    "1-0:1.8.2": "electricity_delivered_2",
    "1-0:2.8.1": "electricity_returned_1",
    "1-0:2.8.2": "electricity_returned_2",
    "1-0:1.7.0": "electricity_currently_delivered",
    "1-0:2.7.0": "electricity_currently_returned",
    "1-0:32.7.0": "phase_voltage_l1",
    # Which tariff band the meter is currently registering to: 1 = low/off-peak, 2 = normal/peak.
    # Worth having as its own column rather than inferring from the clock, which quietly goes wrong
    # around DST and whenever the supplier's schedule changes. Note it says what the *meter* is
    # doing, which is the thing that actually bills.
    "0-0:96.14.0": "electricity_tariff",
    # Actual amps on L1. Whole numbers only -- this meter reports 1 A resolution (confirmed against
    # 30 days of history: 22 distinct values across a 1-22 A range), so it is useful for spotting a
    # near-limit load, not for fine measurement.
    "1-0:31.7.0": "phase_power_current_l1",
}

# Fields the meter reports as whole numbers and that are meaningless as floats -- a tariff band of
# 2.0 groups and indexes worse than 2, and amps arrive at 1 A resolution anyway. Named to match
# DSMR-reader's own columns for the same two values, both of which are integers there too.
_ELEC_INT_FIELDS = {"electricity_tariff", "phase_power_current_l1"}


def _parse_dsmr_timestamp(raw):
    """DSMR's YYMMDDhhmmss+[SW] timestamp -> epoch nanoseconds.

    The S(ummer)/W(inter) suffix is what disambiguates the autumn DST fall-back, the one hour a
    year every local wall-clock time occurs twice: first as CEST (flagged S), then again as CET
    (flagged W). A naive datetime carries fold=0, so astimezone() resolves that hour to the *first*
    occurrence unconditionally -- which made the winter pass produce timestamps an hour early,
    colliding exactly with the summer pass, and write_postgres' ON CONFLICT (time, source) DO
    UPDATE then silently overwrote an hour of electricity and gas readings once a year. Mapping W
    to fold=1 selects the second occurrence instead.

    Per PEP 495 fold is ignored for any unambiguous time, so this is a no-op for the whole rest of
    the year (including the spring-forward gap, whose times a meter never emits) -- verified in
    smoke_test.py against real epochs rather than against this function's own arithmetic.
    """
    match = _DSMR_TIMESTAMP_RE.match(raw)
    if not match:
        raise ValueError(f"Not a DSMR timestamp: {raw!r}")
    year, month, day, hour, minute, second = (int(g) for g in match.groups()[:6])
    naive = datetime(2000 + year, month, day, hour, minute, second,
                     fold=1 if match.group(7) == "W" else 0)
    return int(naive.astimezone().timestamp() * 1_000_000_000)


def _parse_dsmr_number(raw):
    return float(raw.split("*", 1)[0])


def parse_p1_telegram(text):
    """Parses one CRC-verified telegram into its electricity and (if present) gas readings.

    Returns (electricity_values, electricity_ts_ns, gas_reading_or_None), where gas_reading is
    (value, ts_ns) using the M-Bus capture time, not the telegram's own timestamp -- the gas
    meter is only re-sampled every ~5 minutes, so this is what lets _handle_p1_telegram collapse
    the ~1s-repeated readings between updates into one write per real change (see DSMR-MQTT.md's
    `read_at` note, which this mirrors for the old MQTT path).

    The gas M-Bus channel isn't fixed -- discovered by scanning for the device-type line
    (`0-<n>:24.1.0(003)`, device type 003 = gas) the way DSMR-reader itself does, rather than
    assuming channel 1.

    Raises ValueError if the telegram has no 0-0:1.0.0 timestamp line -- that would mean this
    isn't a real P1 telegram.
    """
    elec = {}
    elec_ts_ns = None
    gas_channel = None
    gas_reading = None

    for line in text.splitlines():
        match = _OBIS_LINE_RE.match(line.strip())
        if not match:
            continue
        code, groups = match.group(1), _OBIS_GROUP_RE.findall(match.group(2))
        if not groups:
            continue

        if code == "0-0:1.0.0":
            elec_ts_ns = _parse_dsmr_timestamp(groups[0])
        elif code in _ELEC_OBIS_MAP:
            field = _ELEC_OBIS_MAP[code]
            value = _parse_dsmr_number(groups[0])
            elec[field] = int(value) if field in _ELEC_INT_FIELDS else value
        elif gas_channel is None and code.endswith(":24.1.0") and groups[0] == "003":
            gas_channel = code.split(":")[0].rsplit("-", 1)[1]
        elif gas_channel is not None and code == f"0-{gas_channel}:24.2.1" and len(groups) >= 2:
            gas_reading = (_parse_dsmr_number(groups[1]), _parse_dsmr_timestamp(groups[0]))

    if elec_ts_ns is None:
        raise ValueError("Telegram has no 0-0:1.0.0 timestamp line")
    return elec, elec_ts_ns, gas_reading


_elec_seen = False
_gas_seen = False


def get_elec_values():
    """Latest P1 electricity reading, or None if there isn't one yet or it's gone stale."""
    with _elec_lock:
        cache = _elec_cache
    if cache is None:
        return None
    values, seen = cache
    if time.monotonic() - seen > P1_MAX_DATA_AGE:
        return None
    return values


def _handle_p1_telegram(text):
    global _elec_cache, _elec_seen, _last_elec_write_time, _last_gas_written, _gas_seen
    try:
        elec, elec_ts_ns, gas_reading = parse_p1_telegram(text)
    except Exception:
        log.exception("Could not parse P1 telegram")
        return

    if elec:
        if not _elec_seen:
            _elec_seen = True
            log.info("Receiving electricity data from P1 telegrams: %s", ", ".join(sorted(elec)))
        # The cache updates on every telegram regardless of throttling below, so PVOutput and
        # power_flow always see the latest reading even when what's stored is throttled.
        with _elec_lock:
            _elec_cache = (elec, time.monotonic())

        now = time.monotonic()
        if not P1_MIN_INTERVAL or now - _last_elec_write_time >= P1_MIN_INTERVAL:
            _last_elec_write_time = now
            queue_write(elec, elec_ts_ns, ELEC_TABLE, P1_SOURCE)

            # Published on the same throttle as the Postgres write rather than on every telegram.
            # Telegrams arrive ~1/s, finer than a dashboard or home-automation subscriber needs, and
            # this keeps the published cadence equal to the inverter side (P1_MIN_INTERVAL defaults
            # to SCAN_INTERVAL) so the two topic trees stay in step.
            if ENABLE_MQTT_PUBLISH and _mqtt_client is not None:
                try:
                    publish_mqtt(_mqtt_client, elec, MQTT_ELEC_TOPIC_PREFIX)
                except Exception:
                    log.exception("MQTT publish of electricity readings failed")

            # Also refresh power_flow here, not just from the battery poll loop, so grid and house
            # move at telegram cadence and grid_w keeps going when the battery can't be reached. Gated by the same throttle as the electricity write above -- it's derived
            # from the same reading, so there's no point storing it any more often.
            if ENABLE_DERIVED:
                try:
                    flow = derived_fields(_battery_values_for_derived(), elec)
                    # Can't be empty in practice (elec is truthy here, so grid_w is always set), but
                    # a point with no fields at all would be a row of nothing but a timestamp.
                    if flow:
                        queue_write(flow, elec_ts_ns, DERIVED_TABLE, "derived")
                except Exception:
                    log.exception("Could not compute derived power flow from P1 update")

    if gas_reading is not None:
        value, gas_ts_ns = gas_reading
        if not _gas_seen:
            _gas_seen = True
            log.info("Receiving gas readings from P1 telegrams: delivered=%s m3", value)
        # The meter re-samples gas only every ~5 minutes but the telegram repeats the same
        # reading every ~1s in between -- skip the write when nothing has actually moved.
        if _last_gas_written != (gas_ts_ns, value):
            _last_gas_written = (gas_ts_ns, value)
            queue_write({"delivered": value}, gas_ts_ns, GAS_TABLE, P1_SOURCE)


def _handle_plug_message(message, label):
    try:
        payload = json.loads(message.payload.decode("utf-8"))
    except Exception:
        log.exception("Could not parse plug payload on %s", message.topic)
        return

    values = {}
    for field in PLUG_FIELDS:
        if field not in payload:
            continue
        try:
            values[field] = float(payload[field])
        except (TypeError, ValueError):
            log.debug("Ignoring non-numeric plug field %s=%r on %s", field, payload[field], message.topic)
    if "state" in payload:
        values["state"] = str(payload["state"])
    if not values:
        return

    if label not in _plug_seen:
        _plug_seen.add(label)
        log.info("Receiving plug data for %s on %s: %s", label, message.topic, ", ".join(sorted(values)))

    now = time.monotonic()
    if PLUG_MIN_INTERVAL and now - _plug_last_write_time.get(label, 0.0) < PLUG_MIN_INTERVAL:
        return
    _plug_last_write_time[label] = now

    queue_write(values, time.time_ns(), PLUG_TABLE, label)


def _on_mqtt_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code != 0:
        log.warning("MQTT connection refused: %s", reason_code)
        return
    log.info("MQTT connected to %s:%s", MQTT_HOST, MQTT_PORT)
    if ENABLE_PLUGS:
        # (Re)subscribe on every connect, so a broker restart doesn't silently leave us deaf.
        # QoS 1: at QoS 0 the broker is free to discard messages for a consumer that is slow to
        # drain its socket, which loses readings outright.
        for topic in PLUG_TOPIC_MAP:
            client.subscribe(topic, qos=1)
        log.info("Subscribed to plug topics: %s", ", ".join(PLUG_TOPIC_MAP))


def _on_mqtt_disconnect(client, userdata, flags, reason_code, properties=None):
    log.warning("MQTT disconnected (%s); paho will keep retrying in the background", reason_code)


def mqtt_connect():
    """Starts an MQTT client that connects in the background.

    connect_async means a broker that's down at startup (or restarted later) never takes the
    inverter polling or Postgres writes down with it -- paho retries on its own network thread,
    and publishes while disconnected are dropped rather than raising.
    """
    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = _on_mqtt_connect
    client.on_disconnect = _on_mqtt_disconnect
    client.on_message = _on_mqtt_message
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    return client


def mqtt_watchdog_loop(mqtt_client):
    """Warns while the broker is unreachable.

    paho's connect_async retries silently forever, so without this a blocked port or wrong host is
    indistinguishable from a healthy connection -- nothing is logged either way.
    """
    warned = False
    while True:
        time.sleep(MQTT_WARN_AFTER)
        if mqtt_client.is_connected():
            warned = False
            continue
        if not warned:
            log.warning(
                "Still not connected to MQTT at %s:%s after %ss. Solar values are not being "
                "published and no smart-meter data is arriving. Check the broker is reachable "
                "from this container (firewall/routing) and that the credentials are right.",
                MQTT_HOST,
                MQTT_PORT,
                MQTT_WARN_AFTER,
            )
            warned = True


def _battery_values_for_derived():
    """The cached PV power and AC output for power_flow, or {} when the last poll is stale.

    Stale means unknown: the unit answers around the clock (0 W PV at night is a real reading
    arriving through the normal path), so a poll older than SOLAR_STALE_AFTER means contact was
    lost, and power_flow then omits solar_w and house_w rather than guessing. That is how the gap
    shows in Grafana instead of as a plausible-looking 0, the same as for a P1 outage.
    """
    with _state_lock:
        age = None if latest_poll_monotonic is None else time.monotonic() - latest_poll_monotonic
        cached = {k: latest_values.get(k) for k in ("total_active_power", "ac_output_power", "battery_power")}
    if age is None or age > SOLAR_STALE_AFTER:
        return {}
    return {k: v for k, v in cached.items() if v is not None}


def derived_fields(values, elec):
    """Power flow at one instant: what the panels produce, what the grid does, what the house uses.

    grid_w is signed the way a meter reads: positive drawing from the grid, negative feeding back.
    house_w = grid_w + the battery unit's AC output: the panels feed the house only through the
    unit, so its AC output is everything the house gets that isn't from the grid, whether it came
    from the panels directly or out of the cells. (Before the battery it was solar + grid.) The
    battery terms go into `extra` so the formula can be checked against the stored row. Limit:
    10208 only counts output (it stays 0 while the unit charges from AC), so charging from the grid
    shows up as house use here; in self-consumption mode the unit only charges from the panels.

    Every field is omitted rather than zeroed when its inputs aren't known: no fresh electricity
    drops grid_w and house_w, a stale battery poll drops solar_w and house_w.
    """
    fields = {}
    solar = values.get("total_active_power")
    ac_output = values.get("ac_output_power")
    if solar is not None:
        fields["solar_w"] = float(solar)
    if ac_output is not None:
        fields["battery_ac_w"] = float(ac_output)
    if values.get("battery_power") is not None:
        fields["battery_w"] = float(values["battery_power"])
    if elec:
        imported = METRICS["grid_import_w"][1](values, elec)
        exported = METRICS["grid_export_w"][1](values, elec)
        fields["grid_w"] = float(imported - exported)
        if ac_output is not None:
            fields["house_w"] = float(METRICS["house_power_w"][1](values, elec))
    return fields


def queue_write(values, ts_ns, table, source):
    """Hands a point to the writer thread, dropping the oldest if the queue is saturated."""
    item = (dict(values), ts_ns, table, source)
    try:
        _write_queue.put_nowait(item)
        return
    except queue.Full:
        pass
    try:
        _write_queue.get_nowait()
        # The evicted point will never be written. Count it as done, so unfinished_tasks keeps
        # meaning "queued or being written" -- drain_write_queue() waits on exactly that.
        _write_queue.task_done()
    except queue.Empty:
        pass
    log.warning("Postgres write queue full; dropped the oldest queued point")
    try:
        _write_queue.put_nowait(item)
    except queue.Full:
        # Only reachable if another producer refilled the slot between the get_nowait above and
        # here. Log it separately: the warning above says the *oldest* point was dropped, which
        # would be actively misleading here -- the point being dropped is the new one, and the
        # old one that was evicted to make room is gone too.
        log.warning("Postgres write queue still full after evicting; dropped the new point instead")


PG_RETRY_MAX_BACKOFF = 30.0


def _reset_pg_conn():
    """Drops the writer's persistent connection so the next write opens a fresh one."""
    global _pg_conn
    try:
        if _pg_conn is not None:
            _pg_conn.close()
    except Exception:
        pass
    _pg_conn = None


def _write_until_done(values, ts_ns, table, source, sleep=time.sleep):
    """Writes one point, retrying for as long as the database is unreachable.

    Returns True once written, False if the point itself was rejected and dropped.

    The writer used to take a point off the queue and discard it when the write failed, so a
    database outage threw away everything polled during it while the queue -- sized precisely to
    ride out an outage -- never filled. Now the failure type decides:
      * psycopg.OperationalError / InterfaceError: the connection is the problem (server down,
        restarting, unreachable, admin shutdown, deadlock). The point is kept and retried with a
        1s -> PG_RETRY_MAX_BACKOFF backoff on a fresh connection. New points pile up behind it in
        _write_queue, whose drop-oldest policy is what bounds a really long outage.
      * anything else (DataError, ProgrammingError, e.g. a column the table doesn't have): the
        point is the problem and no retry can fix it. It is dropped so it can't wedge the writer
        and every point queued behind it.
    `sleep` is injectable so the smoke test can run the retry path without waiting.
    """
    backoff = 1.0
    failed_since = None
    while True:
        try:
            write_postgres(values, ts_ns, table=table, source=source)
            if failed_since is not None:
                log.info("Postgres reachable again after %.0fs; %d point(s) queued behind this one",
                         time.monotonic() - failed_since, _write_queue.qsize())
            return True
        except (psycopg.OperationalError, psycopg.InterfaceError) as exc:
            if failed_since is None:
                failed_since = time.monotonic()
                log.warning("Postgres unreachable (%s); holding the point and retrying", exc)
            else:
                log.debug("Postgres still unreachable (%s); retrying in %.0fs", exc, backoff)
            _reset_pg_conn()
            sleep(backoff)
            backoff = min(backoff * 2, PG_RETRY_MAX_BACKOFF)
        except Exception:
            log.exception("Postgres rejected a point for %s; dropping it", table)
            return False


def pg_writer_loop():
    """Drains _write_queue.

    Every Postgres write goes through here so no producer ever blocks on it. A write is a network
    round trip, which would otherwise stretch the poll interval and stall paho's network thread.
    Retry-versus-drop is decided per point in _write_until_done().
    """
    while True:
        values, ts_ns, table, source = _write_queue.get()
        try:
            _write_until_done(values, ts_ns, table, source)
        finally:
            # Written or rejected, this point is finished: see drain_write_queue().
            _write_queue.task_done()


# How long a requested stop waits for queued points to reach Postgres before exiting anyway.
# Keep it below the stop grace period of whatever runs this (`docker stop` allows 10s by default).
SHUTDOWN_DRAIN_TIMEOUT = env_int("SHUTDOWN_DRAIN_TIMEOUT", 5)


def drain_write_queue(timeout):
    """Waits up to `timeout` seconds for the writer thread to finish every queued point.

    Returns how many are still unwritten: 0 means everything reached Postgres. A point counts
    from queue_write() until pg_writer_loop() has written or rejected it, so one that is off the
    queue but still in flight is waited for too. During a database outage this simply runs out
    the timeout; those points were going to be lost on exit either way.
    """
    deadline = time.monotonic() + timeout
    while _write_queue.unfinished_tasks and time.monotonic() < deadline:
        time.sleep(0.05)
    return _write_queue.unfinished_tasks


class _StopRequested(BaseException):
    """Raised in the main thread by the SIGTERM/SIGINT handler.

    A BaseException on purpose: the poll loop catches Exception around every cycle to survive
    inverter and network errors, and a stop request must not be swallowed as one of those.
    """


def _request_stop(signum, frame):
    """Signal handler. Unwinds the main thread out of whatever it is blocked in (the interval
    sleep, a Modbus read with a 10s timeout) instead of only setting a flag it would notice later.

    This process is PID 1 in its container, and the kernel does not apply a signal's default
    action to PID 1: without a handler SIGTERM was ignored outright, so every `docker stop` waited
    out its grace period and then killed the process, along with whatever was still queued.
    """
    raise _StopRequested(signal.Signals(signum).name)


def shutdown(reason):
    """Flushes the write queue (bounded by SHUTDOWN_DRAIN_TIMEOUT) and returns the exit status."""
    # A second signal during the flush must not raise out of it.
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    log.info("Stop requested (%s); flushing queued Postgres writes", reason)
    left = drain_write_queue(SHUTDOWN_DRAIN_TIMEOUT)
    if left:
        log.warning("Exiting with %d point(s) not written to Postgres (still unwritten after %ss)",
                    left, SHUTDOWN_DRAIN_TIMEOUT)
    else:
        log.info("Write queue flushed; exiting")
    return 0


def sync_monta(client=None):
    """Fetch completed Monta sessions and upsert them into MONTA_TABLE in one transaction.
    Returns the number of sessions written. `client` is injectable for the smoke test."""
    client = client or stroohm_monta_api.Monta(MONTA_CLIENT_ID, MONTA_CLIENT_SECRET)
    since = (datetime.now(timezone.utc) - timedelta(days=MONTA_LOOKBACK_DAYS)
             if MONTA_LOOKBACK_DAYS > 0 else None)
    rows = []
    for s in client.sessions(vat_rate=MONTA_VAT_RATE, from_date=since):
        if s["started_at"] is None or s["energy_kwh"] is None:
            log.warning("Skipping Monta session %s without a start time or energy", s["id"])
            continue
        rows.append([json.dumps(s["raw"]) if c == "raw" else s[c] for c in MONTA_COLUMNS])

    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in MONTA_COLUMNS[1:])
    sql = (f"INSERT INTO {MONTA_TABLE} ({', '.join(MONTA_COLUMNS)}, fetched_at) "
           f"VALUES ({', '.join(['%s'] * len(MONTA_COLUMNS))}, now()) "
           f"ON CONFLICT (id) DO UPDATE SET {updates}, fetched_at = now()")
    with psycopg.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD,
    ) as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, row)
    log.info("Monta: upserted %d session(s) into %s", len(rows), MONTA_TABLE)
    return len(rows)


def latest_monta_slot(now, times):
    """The most recent scheduled run time at or before `now` (naive local), from today's or
    yesterday's MONTA_TIMES -- never None, since yesterday's are always in the past."""
    return max(
        datetime.combine(day, datetime.min.time()).replace(hour=h, minute=m)
        for day in (now.date(), now.date() - timedelta(days=1))
        for h, m in times
        if datetime.combine(day, datetime.min.time()).replace(hour=h, minute=m) <= now
    )


def monta_loop():
    """Runs sync_monta() once at startup, then at each of MONTA_TIMES (local) every day.

    A run is due when the latest scheduled slot hasn't been served yet -- polling the wall clock
    each minute, like mindergas_loop, so a restart, clock jump or DST change can't skip a slot or
    run one twice. Unlike Mindergas, an extra run is harmless (an idempotent upsert of the same
    sessions), so it also runs straight away at startup, which makes a first deploy or a config
    fix take effect immediately; that startup run counts as the slot just passed, so it isn't
    repeated. A failure retries every MONTA_RETRY_INTERVAL until it succeeds.
    """
    served = None
    while True:
        now = datetime.now()
        slot = latest_monta_slot(now, MONTA_TIMES)
        if slot != served:
            try:
                sync_monta()
                served = slot
            except Exception:
                log.exception("Monta sync failed; retrying in %ss", MONTA_RETRY_INTERVAL)
                time.sleep(MONTA_RETRY_INTERVAL)
                continue
        time.sleep(60)


def pvoutput_loop():
    """Fires on PVOUTPUT_INTERVAL-aligned wall-clock boundaries (:00/:05/:10 for the 300s
    default), not on a timer counted from container start -- otherwise every restart shifts the
    upload times to a new offset, which is exactly the kind of discontinuity that confused
    PVOutput's own day-total tracking during the PVOutput cutover. Same reasoning as
    mindergas_loop's wall-clock polling.
    """
    if PVOUTPUT_INTERVAL <= 0:
        # Guard rather than divide: 0 reads naturally as "disable", which is exactly what it means
        # for P1_MIN_INTERVAL/PLUG_MIN_INTERVAL, and the bucket arithmetic below would otherwise
        # raise ZeroDivisionError and take this thread down while the process stayed healthy.
        log.warning(
            "PVOUTPUT_INTERVAL is %s, so no PVOutput uploads will be sent. Unset ENABLE_PVOUTPUT "
            "to disable this deliberately, or set a positive interval.", PVOUTPUT_INTERVAL
        )
        return
    last_bucket = int(time.time() // PVOUTPUT_INTERVAL)
    while True:
        time.sleep(1)
        bucket = int(time.time() // PVOUTPUT_INTERVAL)
        if bucket == last_bucket:
            continue
        last_bucket = bucket
        with _state_lock:
            values = dict(latest_values)
            age = None if latest_poll_monotonic is None else time.monotonic() - latest_poll_monotonic
        if not values or age is None:
            continue
        if age > PVOUTPUT_MAX_DATA_AGE:
            log.warning("Skipping PVOutput push: last successful poll was %.0fs ago", age)
            continue
        try:
            push_pvoutput(values)
        except Exception:
            log.exception("PVOutput push failed")


def mindergas_loop():
    """Fires once per day once the clock reaches MINDERGAS_HOUR:MINDERGAS_MINUTE.

    Polls the wall clock rather than sleeping until the target so a container start, clock jump
    or DST change can't skip or double-fire the upload.
    """
    completed_for = None
    while True:
        now = datetime.now()
        # Deliberately scoped to the target hour, so a restart later in the day doesn't fire an
        # unexpected extra upload.
        due = now.hour == MINDERGAS_HOUR and now.minute >= MINDERGAS_MINUTE
        if due and completed_for != now.date():
            try:
                push_mindergas()
                completed_for = now.date()
            except Exception:
                log.exception("Mindergas push failed; retrying in %ss", MINDERGAS_RETRY_INTERVAL)
                time.sleep(MINDERGAS_RETRY_INTERVAL)
                continue
        time.sleep(60)


_relay_lock = threading.Lock()
_relay_clients = set()


def p1_relay_accept_loop():
    """Accepts downstream connections on P1_RELAY_BIND:P1_RELAY_PORT and adds each to
    _relay_clients -- p1_reader_loop's recv loop is what actually writes to them (see
    _relay_broadcast), this thread only handles new arrivals so a slow/stuck accept() can't
    interfere with ingestion.

    Every step retries rather than raising, because an exception escaping here kills this thread
    while the process itself stays healthy: the container keeps polling, keeps writing, keeps
    reporting `Up` to Docker and to Telegraf's container monitoring, and nothing anywhere says the
    relay is gone -- but whatever reads the telegram stream from it (DSMR-reader) has silently lost
    its feed, which is the exact failure this relay was built to end. So a bind that loses a race
    with the outgoing container over :2000 during a recreate retries instead of giving up, and a
    persistently failing accept() sleeps rather than spinning a core at 100% and emitting one
    traceback per iteration (fd exhaustion in particular fails instantly and forever).
    """
    server = None
    while server is None:
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((P1_RELAY_BIND, P1_RELAY_PORT))
            server.listen(5)
        except Exception:
            server = None
            log.exception(
                "Could not bind the P1 relay to %s:%s; retrying in %ss. Anything reading the "
                "telegram stream from this container gets nothing until this succeeds.",
                P1_RELAY_BIND, P1_RELAY_PORT, RELAY_RETRY_INTERVAL,
            )
            time.sleep(RELAY_RETRY_INTERVAL)
    log.info("P1 relay listening on %s:%s", P1_RELAY_BIND, P1_RELAY_PORT)
    while True:
        try:
            conn, addr = server.accept()
            conn.settimeout(P1_RELAY_CLIENT_TIMEOUT)
            with _relay_lock:
                _relay_clients.add(conn)
                connected = len(_relay_clients)
            log.info("P1 relay: new downstream client %s:%s (now %d connected)", addr[0], addr[1], connected)
        except Exception:
            log.exception("P1 relay accept failed; retrying in %ss", RELAY_RETRY_INTERVAL)
            time.sleep(RELAY_RETRY_INTERVAL)


def _relay_broadcast(chunk):
    """Writes `chunk` to every connected relay client, dropping any that error or time out.

    Runs on p1_reader_loop's own thread, right after each recv() -- a bounded per-client send
    timeout (P1_RELAY_CLIENT_TIMEOUT) is what keeps a downstream client that stops reading from
    blocking this container's own ingestion indefinitely; worst case it costs one timeout's worth
    of delay per broadcast, then that client is gone.
    """
    with _relay_lock:
        clients = list(_relay_clients)
    dead = []
    for conn in clients:
        try:
            conn.sendall(chunk)
        except Exception:
            dead.append(conn)
    if dead:
        with _relay_lock:
            for conn in dead:
                _relay_clients.discard(conn)
        for conn in dead:
            try:
                conn.close()
            except Exception:
                pass


def _drain_p1_buffer(buffer):
    """Extracts and handles every complete telegram in `buffer`. Returns (remaining bytes, number
    of telegrams handled). Keeps going as long as the buffer shrinks: a successful parse *and* a
    discarded CRC failure both consume bytes, so this terminates once nothing more can be extracted
    from what has been received so far."""
    handled = 0
    while True:
        prev_len = len(buffer)
        telegram, buffer = extract_telegram(buffer)
        if telegram is not None:
            handled += 1
            try:
                _handle_p1_telegram(telegram)
            except Exception:
                log.exception("Could not process P1 telegram")
        if len(buffer) == prev_len:
            return buffer, handled  # no more progress possible until more bytes arrive


def p1_http_reader_loop():
    """Polls P1_URL every P1_POLL_INTERVAL seconds and processes the telegram it returns, when it
    differs from the previous one.

    Each poll is a fresh request, so the stall problem p1_reader_loop guards against (a connection
    that stays open and says nothing) can't happen here; what can happen is the dongle answering
    with the same telegram forever (it lost the meter) or not answering at all. Both are reported
    once after P1_WARN_AFTER seconds without a new telegram, and once more when telegrams return.
    The response is relayed whole, so ENABLE_P1_RELAY clients get one complete telegram per new
    reading, as they would from the meter itself.
    """
    last_body = None
    last_new = time.monotonic()
    warned = False
    session = requests.Session()
    while True:
        started = time.monotonic()
        try:
            resp = session.get(P1_URL, timeout=max(P1_POLL_INTERVAL, 5))
            resp.raise_for_status()
            body = resp.content[:P1_MAX_BUFFER_BYTES]
        except Exception as exc:
            body = None
            error = exc
        if body is not None and body != last_body:
            last_body = body
            if ENABLE_P1_RELAY:
                _relay_broadcast(body)
            _, handled = _drain_p1_buffer(body)
            if handled:
                last_new = time.monotonic()
                if warned:
                    log.warning("P1 telegrams from %s are arriving again", P1_URL)
                    warned = False
        silent_for = time.monotonic() - last_new
        if not warned and silent_for >= P1_WARN_AFTER:
            log.warning(
                "No new P1 telegram from %s in %.0fs (%s)", P1_URL, silent_for,
                f"last request failed: {error}" if body is None else "the response did not change"
                " or held no valid telegram",
            )
            warned = True
        time.sleep(max(0.0, P1_POLL_INTERVAL - (time.monotonic() - started)))


def p1_reader_loop():
    """Connects to the P1 relay (P1_HOST:P1_PORT) and processes telegrams as they arrive,
    reconnecting with exponential backoff on any failure -- including a connection that stays open
    but stops delivering.

    That last case is the one worth spelling out. A recv() timeout used to mean "keep waiting",
    which is only right if silence is temporary. It isn't always: ser2net can hang while holding
    the TCP session open, and a firewall or router reload can drop conntrack state for the flow --
    in both cases no FIN and no RST ever arrive. Nothing else would notice, because this side only
    ever reads and never writes, so recv() would block in P1_WARN_AFTER slices forever. Ingestion
    stopped permanently, behind a *single* warning (the log was suppressed after the first, and only
    ever reset by data actually arriving, which by definition never happened). With ENABLE_P1_RELAY
    that takes the downstream consumer's feed down too, since it is fed from this loop's own reads.

    So silence is now bounded: P1_STALL_TIMEOUT without a single byte means the connection is dead
    regardless of what the socket claims, and it gets torn down and rebuilt. This deliberately
    replaces TCP keepalive rather than adding to it -- keepalive only proves the *peer* is alive,
    which is exactly what a hung ser2net would keep doing while sending nothing.

    A fresh connection can land mid-telegram, and a torn read can leave a partial one sitting in
    the buffer indefinitely -- both are handled by extract_telegram, called in an inner loop that
    keeps draining `buffer` as long as it's shrinking (a successful parse *or* a discarded
    CRC-failure both still consume bytes, so this reliably terminates once nothing more can be
    extracted from what's been received so far).
    """
    delay = 1
    while True:
        try:
            # Wake up often enough to evaluate the stall deadline even if P1_WARN_AFTER is set
            # longer than it.
            with socket.create_connection((P1_HOST, P1_PORT), timeout=10) as sock:
                sock.settimeout(min(P1_WARN_AFTER, P1_STALL_TIMEOUT))
                log.info("Connected to P1 relay at %s:%s", P1_HOST, P1_PORT)
                warned = False
                buffer = b""
                last_data = time.monotonic()
                while True:
                    try:
                        chunk = sock.recv(4096)
                    except socket.timeout:
                        silent_for = time.monotonic() - last_data
                        if silent_for >= P1_STALL_TIMEOUT:
                            # Raise rather than continue: this drops into the reconnect path below,
                            # which is the only thing that can recover a wedged upstream.
                            raise OSError(
                                f"No P1 data for {silent_for:.0f}s (limit {P1_STALL_TIMEOUT}s); "
                                f"treating the connection as dead and reconnecting"
                            )
                        if not warned:
                            log.warning(
                                "No P1 telegram received in %ss from %s:%s; reconnecting if this "
                                "reaches %ss", P1_WARN_AFTER, P1_HOST, P1_PORT, P1_STALL_TIMEOUT
                            )
                            warned = True
                        continue
                    if not chunk:
                        raise OSError("P1 relay closed the connection")
                    last_data = time.monotonic()
                    warned = False
                    # Reset the backoff on real data, not merely on a successful connect: an
                    # upstream that accepts the connection and then says nothing is precisely the
                    # stall above, and resetting on connect would retry it every second forever
                    # instead of backing off.
                    delay = 1
                    if ENABLE_P1_RELAY:
                        _relay_broadcast(chunk)
                    buffer, _ = _drain_p1_buffer(buffer + chunk)
        except Exception:
            log.warning(
                "P1 connection to %s:%s failed/dropped, retrying in %ss", P1_HOST, P1_PORT, delay, exc_info=True
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)


def validate_config():
    if MQTT_ENABLED and not MQTT_HOST:
        log.error(
            "MQTT_HOST is required when ENABLE_MQTT_PUBLISH or ENABLE_PLUGS is set (publishing=%s, "
            "plugs=%s)", ENABLE_MQTT_PUBLISH, ENABLE_PLUGS,
        )
        sys.exit(1)
    if not MQTT_ENABLED:
        log.info("MQTT disabled entirely (no publishing, no plug ingestion) -- not connecting to a broker")
    if MQTT_PUBLISH_FIELDS and not ENABLE_MQTT_PUBLISH:
        log.warning("MQTT_PUBLISH_FIELDS is set but ENABLE_MQTT_PUBLISH is false, so nothing is published")
    if ENABLE_MQTT_PUBLISH and not ENABLE_P1:
        log.info(
            "Publishing inverter readings under %s only; %s stays empty because ENABLE_P1 is false",
            MQTT_TOPIC_PREFIX, MQTT_ELEC_TOPIC_PREFIX,
        )
    if ENABLE_MQTT_PUBLISH and MQTT_TOPIC_PREFIX == MQTT_ELEC_TOPIC_PREFIX:
        log.warning(
            "MQTT_TOPIC_PREFIX and MQTT_ELEC_TOPIC_PREFIX are both %r -- inverter and smart-meter "
            "fields will share one topic tree. Field names don't currently collide, so nothing is "
            "lost, but the two are meant to be separable.", MQTT_TOPIC_PREFIX,
        )

    unknown = MQTT_PUBLISH_FIELDS - PUBLISHABLE_FIELDS
    if unknown:
        log.warning(
            "MQTT_PUBLISH_FIELDS contains unknown field(s) %s -- they will never be published. "
            "Known fields: %s",
            ", ".join(sorted(unknown)),
            ", ".join(sorted(PUBLISHABLE_FIELDS)),
        )

    if ENABLE_PVOUTPUT and not (PVOUTPUT_API_KEY and PVOUTPUT_SYSTEM_ID):
        log.error("ENABLE_PVOUTPUT is set but PVOUTPUT_API_KEY / PVOUTPUT_SYSTEM_ID are missing")
        sys.exit(1)

    bad = {p: n for p, n in PVOUTPUT_MAPPING.items() if n not in METRICS}
    if bad:
        log.error(
            "Unknown metric(s) in PVOutput mapping: %s. Available metrics: %s",
            ", ".join(f"{p}={n}" for p, n in sorted(bad.items())),
            ", ".join(sorted(METRICS)),
        )
        sys.exit(1)

    # c1 declares which of v1/v3 are lifetime counters; a mismatch silently corrupts the energy
    # figures PVOutput derives, so surface it loudly rather than letting it upload.
    for param, lifetime_flags in (("v1", (1, 2)), ("v3", (1, 3))):
        name = PVOUTPUT_MAPPING.get(param)
        if not name:
            continue
        is_lifetime = name in LIFETIME_METRICS
        declared = PVOUTPUT_CUMULATIVE_FLAG in lifetime_flags
        if is_lifetime != declared:
            log.warning(
                "%s=%s is a %s value but PVOUTPUT_CUMULATIVE_FLAG=%s declares it %s. "
                "Set the flag to %s, or map %s to a %s metric.",
                param,
                name,
                "lifetime" if is_lifetime else "day-total",
                PVOUTPUT_CUMULATIVE_FLAG,
                "lifetime" if declared else "a day total",
                "1 (both lifetime), 2 (only v1), 3 (only v3)",
                param,
                "day-total" if declared else "lifetime",
            )

    needs_elec = sorted(n for n in PVOUTPUT_MAPPING.values() if METRICS[n][0])
    if needs_elec and not ENABLE_P1:
        log.error(
            "PVOutput mapping needs electricity data (%s) but ENABLE_P1 is false",
            ", ".join(needs_elec),
        )
        sys.exit(1)

    if ENABLE_P1_RELAY and not ENABLE_P1:
        log.error("ENABLE_P1_RELAY is set but ENABLE_P1 is false -- there's no upstream stream to relay")
        sys.exit(1)
    log.info(
        "PVOutput mapping: %s%s",
        ", ".join(f"{p}={n}" for p, n in sorted(PVOUTPUT_MAPPING.items())) or "(none)",
        f" | needs electricity data from P1: {', '.join(needs_elec)}" if needs_elec else "",
    )
    if ENABLE_MINDERGAS and not MINDERGAS_API_KEY:
        log.error("ENABLE_MINDERGAS is set but MINDERGAS_API_KEY is missing")
        sys.exit(1)
    if ENABLE_MINDERGAS and not ENABLE_P1:
        log.error("ENABLE_MINDERGAS is set but ENABLE_P1 is false -- there's no other gas source")
        sys.exit(1)

    if ENABLE_MONTA and not (MONTA_CLIENT_ID and MONTA_CLIENT_SECRET):
        log.error("ENABLE_MONTA is set but MONTA_CLIENT_ID and/or MONTA_CLIENT_SECRET is missing")
        sys.exit(1)
    if ENABLE_MONTA and MONTA_TIMES is None:
        log.error("MONTA_TIMES=%r is not a comma-separated list of HH:MM times", MONTA_TIMES_RAW)
        sys.exit(1)

    if ENABLE_P1 and not (P1_HOST or P1_URL):
        log.error("ENABLE_P1 is set but neither P1_URL nor P1_HOST is")
        sys.exit(1)

    if ENABLE_P1 and P1_URL and P1_HOST:
        log.warning("Both P1_URL and P1_HOST are set; reading P1 from P1_URL, P1_HOST is ignored")

    if ENABLE_P1 and P1_STALL_TIMEOUT == float("inf"):
        log.warning(
            "P1_STALL_TIMEOUT=0 disables the stall reconnect. An upstream that hangs while holding "
            "the connection open will stop ingestion permanently instead of being rebuilt."
        )
    elif ENABLE_P1 and P1_STALL_TIMEOUT <= P1_WARN_AFTER:
        log.warning(
            "P1_STALL_TIMEOUT (%ss) is not above P1_WARN_AFTER (%ss), so a stall reconnects before "
            "it is ever warned about -- the reconnect still works, but the logs won't explain why.",
            P1_STALL_TIMEOUT, P1_WARN_AFTER,
        )

    if ENABLE_PLUGS and not PLUG_TOPIC_MAP:
        log.error("ENABLE_PLUGS is set but PLUG_TOPICS has no valid entries")
        sys.exit(1)


def main():
    log.info(
        "Starting energy-monitor: battery=%s:%s scan_interval=%ss p1=%s(%s) p1_relay=%s(%s:%s) "
        "mqtt_publish=%s(%s,%s) plugs=%s(%d) pvoutput=%s mindergas=%s monta=%s",
        BATTERY_HOST,
        BATTERY_PORT,
        SCAN_INTERVAL,
        ENABLE_P1,
        P1_URL or f"{P1_HOST}:{P1_PORT}",
        ENABLE_P1_RELAY,
        P1_RELAY_BIND,
        P1_RELAY_PORT,
        ENABLE_MQTT_PUBLISH,
        MQTT_TOPIC_PREFIX,
        MQTT_ELEC_TOPIC_PREFIX,
        ENABLE_PLUGS,
        len(PLUG_TOPIC_MAP),
        ENABLE_PVOUTPUT,
        ENABLE_MINDERGAS,
        ENABLE_MONTA,
    )
    validate_config()

    global latest_poll_monotonic, _mqtt_client

    modbus_client = ModbusTcpClient(BATTERY_HOST, port=BATTERY_PORT, timeout=5)
    # Skip the broker entirely when nothing publishes to it and nothing subscribes through it,
    # rather than holding a connection the watchdog would then warn about every MQTT_WARN_AFTER.
    mqtt_client = mqtt_connect() if MQTT_ENABLED else None
    _mqtt_client = mqtt_client  # p1_reader_loop's thread publishes through this too

    if mqtt_client is not None:
        threading.Thread(target=mqtt_watchdog_loop, args=(mqtt_client,), daemon=True).start()
    threading.Thread(target=pg_writer_loop, daemon=True).start()
    threading.Thread(target=pvoutput_loop, daemon=True).start()
    threading.Thread(target=mindergas_loop, daemon=True).start()
    if ENABLE_MONTA:
        threading.Thread(target=monta_loop, daemon=True).start()
    if ENABLE_P1:
        threading.Thread(target=p1_http_reader_loop if P1_URL else p1_reader_loop, daemon=True).start()
    if ENABLE_P1_RELAY:
        threading.Thread(target=p1_relay_accept_loop, daemon=True).start()

    while True:
        cycle_started = time.monotonic()
        try:
            if not modbus_client.connected and not modbus_client.connect():
                raise OSError(f"Could not connect to the battery at {BATTERY_HOST}:{BATTERY_PORT}")

            values, battery = poll_battery(modbus_client)
            ts_ns = time.time_ns()
            daily = daily_yield_kwh(values["total_power_yields"])
            if daily is not None:
                values["daily_power_yields"] = daily
            with _state_lock:
                latest_values.clear()
                latest_values.update(values)
                # The battery terms power_flow and PVOutput need, alongside the solar values.
                for key in ("ac_output_power", "battery_power", "soc"):
                    latest_values[key] = battery[key]
                latest_poll_monotonic = time.monotonic()

            log.info(
                "Poll ok: pv=%sW battery=%sW soc=%s%% ac_out=%sW today=%skWh",
                values["total_active_power"], battery["battery_power"], battery["soc"],
                battery["ac_output_power"], daily,
            )

            # Each sink is independent: a Postgres outage must not stop MQTT updates and vice
            # versa. Writes are queued rather than performed here, so their latency never stretches
            # the poll interval.
            queue_write(values, ts_ns, POSTGRES_TABLE, SOLAR_SOURCE)
            queue_write(battery, ts_ns, BATTERY_TABLE, BATTERY_SOURCE)
            if ENABLE_DERIVED:
                try:
                    queue_write(
                        derived_fields({**values, **battery}, get_elec_values() if ENABLE_P1 else None),
                        ts_ns,
                        DERIVED_TABLE,
                        "derived",
                    )
                except Exception:
                    log.exception("Could not compute derived power flow")
            if ENABLE_MQTT_PUBLISH:
                try:
                    publish_mqtt(mqtt_client, values, MQTT_TOPIC_PREFIX, MQTT_PUBLISH_FIELDS)
                except Exception:
                    log.exception("MQTT publish failed")
        except Exception:
            log.exception("Battery poll failed")
            # pymodbus does not reliably flip `.connected` back to False after a send-time
            # socket error (observed: BrokenPipeError leaves it True), so the `not
            # modbus_client.connected` check above would otherwise never trip again and this
            # loop would retry writes on the same dead socket forever -- seen in production
            # retrying every SCAN_INTERVAL for hours until a manual restart. Explicitly closing
            # here forces a genuine reconnect attempt next cycle regardless of what `.connected`
            # claims. close() on an already-broken socket is itself allowed to raise; that's not
            # a new failure worth surfacing since we're already handling one.
            try:
                modbus_client.close()
            except Exception:
                pass

        # Sleep only for the remainder of the interval, so the cadence stays SCAN_INTERVAL rather
        # than SCAN_INTERVAL plus however long the work took.
        time.sleep(max(0.0, SCAN_INTERVAL - (time.monotonic() - cycle_started)))


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)
    try:
        main()
    except _StopRequested as stop:
        sys.exit(shutdown(stop))
