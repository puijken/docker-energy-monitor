import inspect
import json
import logging
import os
import queue
import re
import socket
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
import psycopg
import requests
from pymodbus.client import ModbusTcpClient

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


INVERTER_HOST = env("INVERTER_HOST", required=True)
INVERTER_PORT = env_int("INVERTER_PORT", 502)
INVERTER_SLAVE_ID = env_int("INVERTER_SLAVE_ID", 1)
SCAN_INTERVAL = env_int("SCAN_INTERVAL", 15)

POSTGRES_HOST = env("POSTGRES_HOST", required=True)
POSTGRES_PORT = env_int("POSTGRES_PORT", 5432)
POSTGRES_DB = env("POSTGRES_DB", "energy")
POSTGRES_USER = env("POSTGRES_USER", required=True)
POSTGRES_PASSWORD = env("POSTGRES_PASSWORD", required=True)
POSTGRES_TABLE = env("POSTGRES_TABLE", "solar")

MQTT_HOST = env("MQTT_HOST", required=True)
MQTT_PORT = env_int("MQTT_PORT", 1883)
MQTT_USERNAME = env("MQTT_USERNAME")
MQTT_PASSWORD = env("MQTT_PASSWORD")
MQTT_TOPIC_PREFIX = env("MQTT_TOPIC_PREFIX", "energy/solar").rstrip("/")
# How long to allow for the broker connection before warning, then re-warning, about it.
MQTT_WARN_AFTER = env_int("MQTT_WARN_AFTER", 60)
# Comma-separated register names to publish (e.g. "total_active_power,run_state"). Empty/unset
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
# cached value -- the inverter goes fully offline overnight (Modbus stops responding entirely, not
# just idling at 0), so without this house/grid would flatline right along with it even though the
# smart meter keeps reporting all night.
ENABLE_DERIVED = env_bool("ENABLE_DERIVED", True)
DERIVED_TABLE = env("DERIVED_TABLE", "power_flow")
# How long a solar poll stays "current" for power_flow purposes. Past this, the inverter is
# treated as not producing (0 W) rather than reusing an increasingly-stale reading -- comfortably
# above SCAN_INTERVAL so a single missed poll doesn't trigger it, well below "asleep all night".
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

# Modbus register map for the Sungrow SG5.0RS (device_type_code 0x2606, nominal 5.0 kW) -- a
# single-phase residential string inverter with no battery or meter, so the hybrid-only
# (SH-series) register blocks don't apply.
#
# Addresses and scaling are Sungrow's own published Modbus protocol definition, as encoded in
# github.com/bohdan-s/SungrowClient (registers-sungrow.yaml) -- referenced for interoperability,
# not vendored code. Addresses are 1-based protocol register numbers; Modbus reads are 0-based,
# hence the -1 in poll_inverter. All of these live in the input-register space (function code 4).
REGISTERS = {
    "daily_power_yields": (5003, "U16", 0.1, "kWh"),
    # Whole kWh: register 5004 carries no scaling factor. Verified against the inverter -- 26261
    # kWh over total_running_time 18169 h is 1.45 kW average while generating, right for a 5 kWp
    # array. The 0.1-scaled twin at 5144 (FINE_TOTAL_YIELD_REGISTER below) sits far enough outside
    # this block that including it here would push a single Modbus read past the 125-register
    # protocol limit, so it's polled separately instead -- see poll_inverter.
    "total_power_yields": (5004, "U32", None, "kWh"),
    "total_running_time": (5006, "U32", None, "h"),
    "internal_temperature": (5008, "S16", 0.1, "C"),
    # Apparent power alongside active power gives power factor as a query-time division.
    # Read 3292 VA against 3300 W active, so this model reports it essentially unity.
    "total_apparent_power": (5009, "U32", None, "VA"),
    "mppt_1_voltage": (5011, "U16", 0.1, "V"),
    "mppt_1_current": (5012, "U16", 0.1, "A"),
    "mppt_2_voltage": (5013, "U16", 0.1, "V"),
    "mppt_2_current": (5014, "U16", 0.1, "A"),
    "total_dc_power": (5017, "U32", None, "W"),
    "phase_a_voltage": (5019, "U16", 0.1, "V"),
    "phase_a_current": (5022, "S16", 0.1, "A"),
    "total_active_power": (5031, "U32", None, "W"),
    # Sits outside the block the other registers fall in, so this widens the single Modbus read
    # by a few registers rather than costing a second round trip. Decoded into run_state below,
    # not stored under this name -- see WORK_STATES.
    "work_state_1": (5038, "U16", None, None),
}
# Deliberately absent: installed_pv_power (5016) reads 0 on this unit rather than the array
# size, so dashboards carry the 6.32 kWp figure as a constant instead. phase_b/c_voltage
# (5020/5021) and mppt_3 (5015/5016) read 0 too -- this is a single-phase, two-string model.
# Every register above already falls inside the one block poll_inverter reads, so none of
# them costs an extra Modbus round trip.

# 0.1 kWh-scaled twin of total_power_yields (register 5004), confirmed live against the inverter
# (26450.8 kWh here vs 26450 unscaled). Read as its own small Modbus block rather than folded into
# REGISTERS/the main scan: 5144 sits far enough past the 5003-5038 block that including it would
# push SCAN_COUNT to 143 registers, over the 125-register limit a single Modbus read PDU allows.
#
# Exists to fix a real problem: dashboard panels that compute a single day's (or the live "so
# far today") generation as max(total_power_yields) - min(total_power_yields) over that period
# inherit total_power_yields' whole-kWh quantization, which shows up as visible 1 kWh jumps when
# the day's total is still small (see dashboard_energy's README). This field isn't in REGISTERS,
# so it isn't a fixed column on the `solar` table -- it lands in `extra` (see TABLE_COLUMNS/
# write_postgres) until it's worth promoting to one.
FINE_TOTAL_YIELD_REGISTER = (5144, "U32", 0.1, "kWh")

# work_state_1's value mapping, same source as REGISTERS above. Not a bitmask despite the range --
# each is a distinct sentinel value, not OR-able flags.
WORK_STATES = {
    0x0000: "Run",
    0x8000: "Stop",
    0x1300: "Key Stop",
    0x1500: "Emergency Stop",
    0x1400: "Standby",
    0x1200: "Initial Standby",
    0x1600: "Starting",
    0x9100: "Alarm Run",
    0x8100: "Derating Run",
    0x8200: "Dispatch Run",
    0x5500: "Fault",
    0x2500: "Communication Fault",
}

SCAN_START_ADDRESS = min(addr for addr, *_ in REGISTERS.values())
SCAN_END_ADDRESS = max(addr + (1 if dtype in ("U32", "S32") else 0) for addr, dtype, *_ in REGISTERS.values())
SCAN_COUNT = SCAN_END_ADDRESS - SCAN_START_ADDRESS + 1

# Fixed columns per table, matching the Postgres schema the deploying stack creates (see its
# timescaledb init SQL). A field not listed here for its table lands in that table's `extra`
# JSONB column instead of failing the insert, so a new Zigbee2MQTT field needs no code change here
# -- only a schema/dashboard change once it's actually wanted.
#
# Note this does NOT extend to smart-meter fields, despite both going through write_postgres:
# parse_p1_telegram maps OBIS codes through the fixed _ELEC_OBIS_MAP allowlist and discards
# everything else before write_postgres ever sees it, so ELEC_TABLE's `extra` is always NULL and a
# new OBIS code does need a code change. Several real fields are dropped this way today (the active
# tariff indicator 0-0:96.14.0, current 1-0:31.7.0, the power-failure and voltage sag/swell
# counters); adding them means extending _ELEC_OBIS_MAP, not just the schema.
TABLE_COLUMNS = {
    POSTGRES_TABLE: set(REGISTERS) | {"run_state"},
    ELEC_TABLE: {
        "electricity_delivered_1", "electricity_returned_1", "electricity_delivered_2",
        "electricity_returned_2", "electricity_currently_delivered",
        "electricity_currently_returned", "phase_voltage_l1",
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
    # --- inverter only (always available) ---
    "daily_generation_wh": (False, lambda i, e: round(i["daily_power_yields"] * 1000)),
    "total_generation_wh": (False, lambda i, e: round(i["total_power_yields"] * 1000)),
    "generation_w": (False, lambda i, e: i["total_active_power"]),
    "dc_power_w": (False, lambda i, e: i["total_dc_power"]),
    "inverter_voltage_v": (False, lambda i, e: i["phase_a_voltage"]),
    # Inverter heatsink temperature, NOT ambient -- runs ~50 C in normal operation, so mapping it
    # to v5 will skew PVOutput's insolation figures. The old SunGather config left it disabled.
    "inverter_temp_c": (False, lambda i, e: i["internal_temperature"]),
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
    # --- derived: what the house actually used (generation + import - export) ---
    "house_power_w": (
        True,
        lambda i, e: round(
            i["total_active_power"]
            + (_e(e, "electricity_currently_delivered") - _e(e, "electricity_currently_returned")) * 1000
        ),
    ),
    "total_house_energy_wh": (
        True,
        lambda i, e: round(
            i["total_power_yields"] * 1000
            + (_e(e, "electricity_delivered_1") + _e(e, "electricity_delivered_2")) * 1000
            - (_e(e, "electricity_returned_1") + _e(e, "electricity_returned_2")) * 1000
        ),
    ),
    # No daily_import_wh/daily_export_wh/daily_house_energy_wh equivalent: raw telegrams carry no
    # day-totals (see the comment above ENABLE_P1), so there's nothing to compute these from.
}

# Metrics that are lifetime counters rather than day totals. PVOutput's c1 flag has to agree with
# whichever of these lands on v1/v3, or the uploaded energy figures come out wrong.
LIFETIME_METRICS = {"total_generation_wh", "total_import_wh", "total_export_wh", "total_house_energy_wh"}

# PVOUTPUT_V1..V6 name a metric each; unset means that parameter is not uploaded. The defaults
# reproduce exactly what the previous SunGather setup sent.
PVOUTPUT_MAPPING = {
    param: name
    for param, name in (
        (param, env(f"PVOUTPUT_{param.upper()}", default))
        for param, default in (("v1", "daily_generation_wh"), ("v2", "generation_w"), ("v3", ""),
                               ("v4", ""), ("v5", ""), ("v6", "inverter_voltage_v"))
    )
    if name
}

_state_lock = threading.Lock()
latest_values = {}
latest_poll_monotonic = None

# Latest electricity reading from the P1 telegram stream, as (values, monotonic_time) -- read by
# build_pvoutput_params/derived_fields, written only by p1_reader_loop's own thread.
_elec_lock = threading.Lock()
_elec_cache = None
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
# same physical constraint applies to any blocking write here). Bounded so a prolonged Postgres
# outage cannot grow without limit; oldest points are dropped first, since fresh readings matter
# more than stale ones.
_write_queue = queue.Queue(maxsize=env_int("WRITE_QUEUE_SIZE", 2000))


def decode_register(words, offset, datatype):
    raw = words[offset]
    if datatype == "U16":
        return 0 if raw == 0xFFFF else raw
    if datatype == "S16":
        if raw in (0xFFFF, 0x7FFF):
            return 0
        return raw - 65536 if raw >= 32767 else raw
    if datatype == "U32":
        low, high = words[offset], words[offset + 1]
        if low == 0xFFFF and high == 0xFFFF:
            return 0
        return low + high * 0x10000
    if datatype == "S32":
        low, high = words[offset], words[offset + 1]
        if low == 0xFFFF and high in (0xFFFF, 0x7FFF):
            return 0
        if high >= 32767:
            return low + high * 0x10000 - 0xFFFFFFFF - 1
        return low + high * 0x10000
    raise ValueError(f"Unsupported datatype {datatype}")


# pymodbus renamed this argument from `slave` to `device_id` (3.14 dropped `slave` outright), so
# resolve it from the signature instead of hardcoding either name. Dependabot auto-merges anything
# it doesn't classify as semver-major, which has already silently moved this pin once.
_UNIT_KWARG = (
    "device_id"
    if "device_id" in inspect.signature(ModbusTcpClient.read_input_registers).parameters
    else "slave"
)


def poll_inverter(client):
    result = client.read_input_registers(
        SCAN_START_ADDRESS - 1, count=SCAN_COUNT, **{_UNIT_KWARG: INVERTER_SLAVE_ID}
    )
    if result.isError():
        raise OSError(f"Modbus read failed: {result}")
    words = result.registers

    values = {}
    for name, (address, datatype, accuracy, _unit) in REGISTERS.items():
        value = decode_register(words, address - SCAN_START_ADDRESS, datatype)
        values[name] = round(value * accuracy, 2) if accuracy else value

    # work_state_1 (see WORK_STATES) turned out to be readable on this model too, despite an
    # earlier assumption that run/system state registers were hybrid-only in Sungrow's protocol --
    # replaces a synthetic "ON"/"OFF" derived from total_active_power > 0 with the inverter's own
    # reported state. Unmapped codes are logged verbatim rather than silently dropped, so a new one
    # shows up instead of just going missing.
    work_state_raw = values.pop("work_state_1")
    values["run_state"] = WORK_STATES.get(work_state_raw, f"Unknown (0x{work_state_raw:04X})")

    # Separate round trip for FINE_TOTAL_YIELD_REGISTER (see its comment) -- optional, so a failure
    # here (a firmware without this register, a mid-poll hiccup) drops only this one field into a
    # log line rather than raising and losing the whole poll, which the main block above is not
    # allowed to do.
    try:
        fine_address, fine_datatype, fine_accuracy, _fine_unit = FINE_TOTAL_YIELD_REGISTER
        fine_result = client.read_input_registers(
            fine_address - 1, count=2, **{_UNIT_KWARG: INVERTER_SLAVE_ID}
        )
        if fine_result.isError():
            raise OSError(f"Modbus read failed: {fine_result}")
        fine_value = decode_register(fine_result.registers, 0, fine_datatype)
        values["total_power_yields_precise"] = round(fine_value * fine_accuracy, 2)
    except Exception:
        log.warning("Could not read fine-grained total yield register (5144)", exc_info=True)

    return values


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


def publish_mqtt(mqtt_client, values):
    for name, value in values.items():
        if MQTT_PUBLISH_FIELDS and name not in MQTT_PUBLISH_FIELDS:
            continue
        mqtt_client.publish(f"{MQTT_TOPIC_PREFIX}/{name}", value, retain=True)


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
}


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
            elec[_ELEC_OBIS_MAP[code]] = _parse_dsmr_number(groups[0])
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

            # Also refresh power_flow here, not just from the solar poll loop -- the smart meter
            # keeps reporting all night even when the inverter is asleep and Modbus stops
            # responding entirely, so this is what keeps house/grid from flatlining along with
            # solar. Gated by the same throttle as the electricity write above -- it's derived
            # from the same reading, so there's no point storing it any more often.
            if ENABLE_DERIVED:
                try:
                    queue_write(
                        derived_fields({"total_active_power": _solar_power_for_derived()}, elec),
                        elec_ts_ns,
                        DERIVED_TABLE,
                        "derived",
                    )
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


def _solar_power_for_derived():
    """The inverter's current output for power_flow purposes: the real value if the last poll is
    still within SOLAR_STALE_AFTER, otherwise 0. Unlike a DSMR outage (unknown, so omitted), a
    stale solar poll reliably means "asleep, not producing" -- 0 is the correct value, not a
    guess.
    """
    with _state_lock:
        age = None if latest_poll_monotonic is None else time.monotonic() - latest_poll_monotonic
        power = latest_values.get("total_active_power")
    if power is None or age is None or age > SOLAR_STALE_AFTER:
        return 0.0
    return power


def derived_fields(values, elec):
    """Power flow at one instant: what the panels produce, what the grid does, what the house uses.

    grid_w is signed the way a meter reads: positive drawing from the grid, negative feeding back.
    Grid and house fields are omitted rather than zeroed when no fresh electricity data is cached,
    so a P1 outage shows as a gap in Grafana instead of a plausible-looking 0 W.
    """
    fields = {"solar_w": float(values["total_active_power"])}
    if elec:
        imported = METRICS["grid_import_w"][1](values, elec)
        exported = METRICS["grid_export_w"][1](values, elec)
        fields["grid_w"] = float(imported - exported)
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
    except queue.Empty:
        pass
    log.warning("Postgres write queue full; dropped the oldest queued point")
    try:
        _write_queue.put_nowait(item)
    except queue.Full:
        pass


def pg_writer_loop():
    """Drains _write_queue.

    Every Postgres write goes through here so no producer ever blocks on it. A write is a network
    round trip, which would otherwise stretch the poll interval and stall paho's network thread.
    """
    while True:
        values, ts_ns, table, source = _write_queue.get()
        try:
            write_postgres(values, ts_ns, table=table, source=source)
        except Exception:
            log.exception("Postgres write failed for %s", table)


def pvoutput_loop():
    """Fires on PVOUTPUT_INTERVAL-aligned wall-clock boundaries (:00/:05/:10 for the 300s
    default), not on a timer counted from container start -- otherwise every restart shifts the
    upload times to a new offset, which is exactly the kind of discontinuity that confused
    PVOutput's own day-total tracking during the PVOutput cutover. Same reasoning as
    mindergas_loop's wall-clock polling.
    """
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


def p1_reader_loop():
    """Connects to the P1 relay (P1_HOST:P1_PORT) and processes telegrams as they arrive,
    reconnecting with exponential backoff on any failure.

    A fresh connection can land mid-telegram, and a torn read can leave a partial one sitting in
    the buffer indefinitely -- both are handled by extract_telegram, called in an inner loop that
    keeps draining `buffer` as long as it's shrinking (a successful parse *or* a discarded
    CRC-failure both still consume bytes, so this reliably terminates once nothing more can be
    extracted from what's been received so far).
    """
    delay = 1
    while True:
        try:
            with socket.create_connection((P1_HOST, P1_PORT), timeout=10) as sock:
                sock.settimeout(P1_WARN_AFTER)
                log.info("Connected to P1 relay at %s:%s", P1_HOST, P1_PORT)
                delay = 1
                warned = False
                buffer = b""
                while True:
                    try:
                        chunk = sock.recv(4096)
                    except socket.timeout:
                        if not warned:
                            log.warning(
                                "No P1 telegram received in %ss from %s:%s", P1_WARN_AFTER, P1_HOST, P1_PORT
                            )
                            warned = True
                        continue
                    if not chunk:
                        raise OSError("P1 relay closed the connection")
                    warned = False
                    if ENABLE_P1_RELAY:
                        _relay_broadcast(chunk)
                    buffer += chunk
                    while True:
                        prev_len = len(buffer)
                        telegram, buffer = extract_telegram(buffer)
                        if telegram is not None:
                            try:
                                _handle_p1_telegram(telegram)
                            except Exception:
                                log.exception("Could not process P1 telegram")
                        if len(buffer) == prev_len:
                            break  # no more progress possible until more bytes arrive
        except Exception:
            log.warning(
                "P1 connection to %s:%s failed/dropped, retrying in %ss", P1_HOST, P1_PORT, delay, exc_info=True
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)


def validate_config():
    unknown = MQTT_PUBLISH_FIELDS - set(REGISTERS) - {"run_state"}
    if unknown:
        log.warning(
            "MQTT_PUBLISH_FIELDS contains unknown field(s) %s -- they will never be published. "
            "Known fields: %s",
            ", ".join(sorted(unknown)),
            ", ".join(sorted(set(REGISTERS) | {"run_state"})),
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

    if ENABLE_P1 and not P1_HOST:
        log.error("ENABLE_P1 is set but P1_HOST is missing")
        sys.exit(1)

    if ENABLE_PLUGS and not PLUG_TOPIC_MAP:
        log.error("ENABLE_PLUGS is set but PLUG_TOPICS has no valid entries")
        sys.exit(1)


def main():
    log.info(
        "Starting energy-monitor: inverter=%s:%s scan_interval=%ss p1=%s(%s:%s) p1_relay=%s(%s:%s) "
        "plugs=%s(%d) pvoutput=%s mindergas=%s",
        INVERTER_HOST,
        INVERTER_PORT,
        SCAN_INTERVAL,
        ENABLE_P1,
        P1_HOST,
        P1_PORT,
        ENABLE_P1_RELAY,
        P1_RELAY_BIND,
        P1_RELAY_PORT,
        ENABLE_PLUGS,
        len(PLUG_TOPIC_MAP),
        ENABLE_PVOUTPUT,
        ENABLE_MINDERGAS,
    )
    validate_config()

    global latest_poll_monotonic

    modbus_client = ModbusTcpClient(INVERTER_HOST, port=INVERTER_PORT, timeout=10)
    mqtt_client = mqtt_connect()

    threading.Thread(target=mqtt_watchdog_loop, args=(mqtt_client,), daemon=True).start()
    threading.Thread(target=pg_writer_loop, daemon=True).start()
    threading.Thread(target=pvoutput_loop, daemon=True).start()
    threading.Thread(target=mindergas_loop, daemon=True).start()
    if ENABLE_P1:
        threading.Thread(target=p1_reader_loop, daemon=True).start()
    if ENABLE_P1_RELAY:
        threading.Thread(target=p1_relay_accept_loop, daemon=True).start()

    while True:
        cycle_started = time.monotonic()
        try:
            if not modbus_client.connected and not modbus_client.connect():
                raise OSError(f"Could not connect to inverter at {INVERTER_HOST}:{INVERTER_PORT}")

            values = poll_inverter(modbus_client)
            ts_ns = time.time_ns()
            with _state_lock:
                latest_values.clear()
                latest_values.update(values)
                latest_poll_monotonic = time.monotonic()

            log.info(
                "Poll ok: ac=%sW dc=%sW daily=%skWh temp=%sC",
                values["total_active_power"],
                values["total_dc_power"],
                values["daily_power_yields"],
                values["internal_temperature"],
            )

            # Each sink is independent: a Postgres outage must not stop MQTT updates (the
            # physical display reads those) and vice versa. The Postgres write is queued rather
            # than performed here, so its latency never stretches the poll interval.
            queue_write(values, ts_ns, POSTGRES_TABLE, "sungrow")
            if ENABLE_DERIVED:
                try:
                    queue_write(
                        derived_fields(values, get_elec_values() if ENABLE_P1 else None),
                        ts_ns,
                        DERIVED_TABLE,
                        "derived",
                    )
                except Exception:
                    log.exception("Could not compute derived power flow")
            try:
                publish_mqtt(mqtt_client, values)
            except Exception:
                log.exception("MQTT publish failed")
        except Exception:
            log.exception("Inverter poll failed")

        # Sleep only for the remainder of the interval, so the cadence stays SCAN_INTERVAL rather
        # than SCAN_INTERVAL plus however long the work took.
        time.sleep(max(0.0, SCAN_INTERVAL - (time.monotonic() - cycle_started)))


if __name__ == "__main__":
    main()
