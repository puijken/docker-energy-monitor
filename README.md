# Energy Monitor

Collects whole-house energy data into one TimescaleDB (Postgres): solar generation and the home
battery read from an Anker SOLIX Solarbank (Max) over Modbus TCP, and electricity/gas consumption
read directly off the P1 smart meter's telegram (from a stream, or from a P1 dongle's HTTP API). Optionally re-publishes solar values to MQTT, and uploads
to PVOutput (every few minutes) and Mindergas (once daily). Built as a self-maintained replacement
for the third-party `bohdans/sungather` image; it read a Sungrow string inverter until that was
replaced by the Solarbank (the Sungrow code is in the git history).

## Features

- Polls the Solarbank every `SCAN_INTERVAL` seconds over Modbus TCP (read-only, function codes 3
  and 4): PV power and lifetime yield, battery power and state of charge, the unit's AC output,
  charge/discharge counters and settings. See [Modbus register map](#modbus-register-map).
- Connects to a plain-TCP relay of the meter's P1 port and parses DSMR telegrams itself
  (CRC-verified) for smart-meter electricity and gas readings, and writes those to the same
  database too — so one place holds generation *and* consumption, with no other project's exporter
  in the path.
- Writes every reading to TimescaleDB (PV under `solar` with `source=sungrow`, kept from the
  previous inverter so the series continues, see [Modbus register map](#modbus-register-map);
  the battery under `battery` with `source=anker`; smart-meter data under
  `electricity` / `gas_positions` with `source=dsmr`). That `source` value names the *meter*, not
  the program reading it, so it stays put across ingestion-path changes — it is a grouping key in
  downstream continuous aggregates, and changing it starts a parallel series instead of continuing
  the existing one. Plug fields not in a table's fixed columns land in an `extra` JSONB column
  instead of failing the write, so a new plug field needs no code change here — see
  [Smart plugs](#smart-plugs-zigbee2mqtt). Electricity is *not* schemaless in the same way: OBIS
  codes are mapped through a fixed allowlist and anything unlisted is dropped at parse time, so a
  new smart-meter field does need one.
- Optionally re-publishes readings to MQTT, one retained topic per field, under two separate
  prefixes: solar data under `MQTT_TOPIC_PREFIX` and smart-meter electricity under
  `MQTT_ELEC_TOPIC_PREFIX`, so a subscriber can take either side alone. Both are gated by the one
  `ENABLE_MQTT_PUBLISH` switch (default off). Outbound only — nothing here reads it back, and
  Postgres is written either way.
- Optionally pushes a status update to PVOutput on an interval (gated by `ENABLE_PVOUTPUT`,
  default off).
- Optionally pushes the previous day's cumulative gas meter reading to Mindergas shortly after
  midnight (gated by `ENABLE_MINDERGAS`, default off), reading the value back out of the
  `gas_positions` table this container itself writes.
- Optionally fetches your EV charging sessions daily from Monta's Public API (a Stroohm charge
  card, or any Monta account) into a table of their own (`ENABLE_MONTA`, default off). See
  [Stroohm / Monta charging sessions](#stroohm--monta-charging-sessions).

All three outbound integrations (`ENABLE_MQTT_PUBLISH`, `ENABLE_PVOUTPUT`, `ENABLE_MINDERGAS`)
default to `false`, so nothing leaves this container unless you opt in and it can run safely
alongside an existing uploader (SunGather, DSMR-reader's own exporters) without double-reporting. Each upload
should be owned by exactly one uploader, so disable the old one at its source before enabling the
matching flag here; the two are independent and can be cut over separately.

`PVOUTPUT_CUMULATIVE_FLAG` (PVOutput's `c1`) defaults to `0` because the default `v1` mapping
(`daily_generation_wh`) is a day total that resets to 0 at midnight, not a true lifetime counter.
Setting it to `1`/`2` for a day-total metric is a real production issue, not just theoretical: it
tells PVOutput to compute Energy Generation as a delta from a baseline it tracks unreliably across
midnight resets, which can leave Energy Generation stuck at 0 all day on PVOutput's site while
Power/Voltage keep reporting fine (see the
[forum thread](https://forum.pvoutput.org/t/c1-1-first-positive-value-is-subtracted-from-all-subsequent-values/8930)).
Only set this flag if you remap `v1`/`v3` to a genuine lifetime metric such as `total_generation_wh`.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `WARNING` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL` (case-insensitive). See [Log levels](#log-levels). |
| `BATTERY_HOST` | *required* | Solarbank IP address (Modbus TCP switched on in the Anker app) |
| `BATTERY_PORT` | `502` | Modbus TCP port |
| `BATTERY_UNIT_ID` | `1` | Modbus unit ID |
| `BATTERY_TABLE` | `battery` | Table for the battery rows |
| `PV_TOTAL_OFFSET_KWH` | `0` | Added to the unit's lifetime PV counter; set to a previous inverter's final total to continue its series |
| `SCAN_INTERVAL` | `15` | Seconds between battery polls |
| `POSTGRES_HOST` | *required* | TimescaleDB/Postgres hostname, e.g. `timescaledb` |
| `POSTGRES_PORT` | `5432` | Postgres port |
| `POSTGRES_DB` | `energy` | Database name |
| `POSTGRES_USER` | *required* | Database user |
| `POSTGRES_PASSWORD` | *required* | Database password |
| `POSTGRES_TABLE` | `solar` | Table name written to for the PV readings |
| `WRITE_QUEUE_SIZE` | `2000` | Max points buffered for the Postgres writer thread. While the database is unreachable the writer holds its current point and retries (1 s → 30 s backoff), so this queue is what bridges the outage; at a few points per second the default covers roughly half an hour. Past that the oldest queued point is dropped first rather than growing without bound. A point the database *rejects* (bad data, unknown column) is logged and dropped at once, so it can't block the ones behind it. |
| `SHUTDOWN_DRAIN_TIMEOUT` | `5` | Seconds a requested stop (`SIGTERM`/`SIGINT`, i.e. `docker stop`) waits for queued points to reach Postgres before exiting anyway. Normally the queue is all but empty and the container stops in well under a second. Keep it below the stop grace period (`docker stop` allows 10 s by default); anything still unwritten when it runs out is logged as a warning and lost, which only happens when the database is unreachable at that moment. |
| `MQTT_HOST` | *required if MQTT is used* | Broker hostname/IP. Only required when `ENABLE_MQTT_PUBLISH` or `ENABLE_PLUGS` is on; with both off the container never connects to a broker at all. |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USERNAME` | *(none)* | MQTT username, if required |
| `MQTT_PASSWORD` | *(none)* | MQTT password, if required |
| `MQTT_WARN_AFTER` | `60` | Seconds without a broker connection before warning it looks unreachable; repeats every interval while still disconnected |
| `ENABLE_MQTT_PUBLISH` | `false` | Re-publish each solar reading to MQTT, one retained topic per field. Purely an outbound copy — Postgres is written either way and nothing here reads it back. **Changed from `true` to `false` on 2026-08-14**: if you are upgrading and something subscribes to these topics, set this to `true` or it goes quiet. Does **not** affect plug ingestion, which shares the same connection. |
| `MQTT_TOPIC_PREFIX` | `energy/solar` | Topic prefix for **solar** readings; one sub-topic per field. |
| `MQTT_ELEC_TOPIC_PREFIX` | `energy/electricity` | Topic prefix for **smart-meter electricity** readings, published at `P1_MIN_INTERVAL` cadence. Only produces anything when `ENABLE_P1` is on. Gas and plug data are not published. |
| `MQTT_PUBLISH_FIELDS` | *(all)* | Comma-separated **solar** field names to publish, e.g. `total_active_power,daily_power_yields`. Leave unset to publish everything. Applies to the inverter topic tree only, not `MQTT_ELEC_TOPIC_PREFIX`. These are the field names as published, which are not quite the register names: use `run_state`, not the raw `work_state_1` it is decoded from, and `total_power_yields_precise` is available even though it is not part of the main register block. Unrecognised names are warned about at startup. |
| `ENABLE_P1` | `false` | Connect to a P1 telegram relay for smart-meter data. See [Smart-meter data](#smart-meter-data). |
| `P1_HOST` | *required if `ENABLE_P1=true` and `P1_URL` is unset* | Host/IP of the P1 telegram relay |
| `P1_PORT` | `2001` | Port of the P1 telegram relay |
| `P1_URL` | *(unset)* | Read the telegram over HTTP from a P1 dongle's local API instead (e.g. HomeWizard P1 meter: `http://<dongle>/api/v1/telegram`). Replaces `P1_HOST`/`P1_PORT`. See [Reading P1 from a dongle's HTTP API](#reading-p1-from-a-dongles-http-api) |
| `P1_POLL_INTERVAL` | `1` | Seconds between polls of `P1_URL` |
| `ELEC_TABLE` | `electricity` | Table for electricity readings |
| `GAS_TABLE` | `gas_positions` | Table for gas readings |
| `P1_WARN_AFTER` | `60` | Seconds without a telegram before warning the connection looks stalled |
| `P1_STALL_TIMEOUT` | `180` | Seconds without a telegram before the connection is torn down and rebuilt, however healthy the socket still looks. `0` disables the reconnect — see [When the connection stalls without dropping](#when-the-connection-stalls-without-dropping) |
| `P1_MAX_DATA_AGE` | `300` | Cached electricity values older than this count as absent |
| `P1_MIN_INTERVAL` | `SCAN_INTERVAL` | Minimum seconds between Postgres writes of electricity data (telegrams arrive roughly once a second); the in-memory cache still updates every telegram regardless, so PVOutput/`power_flow` always see the latest reading |
| `ENABLE_P1_RELAY` | `false` | Re-serve the P1 stream to downstream TCP clients (e.g. DSMR-reader). **Never share ser2net itself between two long-lived clients — use this instead.** See [Sharing the P1 port](#sharing-the-p1-port-with-something-else-that-also-reads-it). |
| `P1_RELAY_BIND` / `P1_RELAY_PORT` | `0.0.0.0` / `2000` | Interface/port the relay listens on |
| `P1_RELAY_CLIENT_TIMEOUT` | `5` | Seconds a send to a downstream relay client may block before it's dropped as dead |
| `ENABLE_DERIVED` | `true` | Write `power_flow` (solar/grid/house at one instant), combining the latest cached battery and P1 electricity readings whenever either updates |
| `DERIVED_TABLE` | `power_flow` | Table written to |
| `SOLAR_STALE_AFTER` | `120` | Seconds after which a battery poll is too old to reuse for `power_flow`; past it the solar and house terms are omitted — see [What a stale battery poll means](#what-a-stale-battery-poll-means). |
| `ENABLE_PLUGS` | `false` | Subscribe to configured Zigbee2MQTT smart-plug topics for per-circuit submetering. See [Smart plugs](#smart-plugs-zigbee2mqtt). |
| `PLUG_TOPICS` | *(none)* | Comma-separated `topic=label` pairs, e.g. `zigbee2mqtt/Fridge plug=fridge,zigbee2mqtt/Office plug=office`. The label becomes the `source` column value. |
| `PLUG_TABLE` | `smart_plugs` | Table written to |
| `PLUG_MIN_INTERVAL` | `5` | Minimum seconds between Postgres writes per plug |
| `ENABLE_PVOUTPUT` | `false` | When `false`, logs what would be sent instead of posting |
| `PVOUTPUT_API_KEY` | *(none)* | Required if `ENABLE_PVOUTPUT=true` |
| `PVOUTPUT_SYSTEM_ID` | *(none)* | Required if `ENABLE_PVOUTPUT=true` |
| `PVOUTPUT_V1`…`PVOUTPUT_V6` | see [PVOutput mapping](#pvoutput-mapping) | Metric name uploaded as each PVOutput parameter; unset = not uploaded |
| `PVOUTPUT_INTERVAL` | `300` | Seconds between PVOutput pushes. Pushes fire on wall-clock-aligned boundaries of this interval (:00/:05/:10… at the default), not on a timer counted from container start, so restarts don't shift the upload times. PVOutput's own rate limit is 60 requests/hour (300 with a donation account), so don't go below 60 |
| `PVOUTPUT_CUMULATIVE_FLAG` | `0` | PVOutput `c1`: `0` (day totals, matches the default `v1` mapping), `1` = v1 and v3 both lifetime, `2` = only v1 lifetime, `3` = only v3 lifetime. Only set to `1`/`2`/`3` if `v1`/`v3` are remapped to a genuine lifetime metric — see the note in `app.py` |
| `PVOUTPUT_MAX_DATA_AGE` | `600` | Skip the upload if the last successful poll is older than this, rather than re-posting a stale reading under the current timestamp |
| `ENABLE_MINDERGAS` | `false` | When `false`, logs what would be sent instead of posting |
| `MINDERGAS_API_KEY` | *(none)* | Required if `ENABLE_MINDERGAS=true` |
| `MINDERGAS_HOUR` / `MINDERGAS_MINUTE` | `0` / `5` | Local time-of-day the daily Mindergas push fires |
| `MINDERGAS_RETRY_INTERVAL` | `900` | Seconds to wait before retrying a failed daily push (retries stay within `MINDERGAS_HOUR`) |
| `MINDERGAS_GAS_TABLE` | `gas_positions` | Table this container's own P1 ingestion writes gas readings to |
| `MINDERGAS_GAS_FIELD` | `delivered` | Column name within that table |
| `ENABLE_MONTA` | `false` | Fetch your EV charging sessions daily from Monta's Public API into `MONTA_TABLE`. See [Stroohm / Monta charging sessions](#stroohm--monta-charging-sessions) |
| `MONTA_CLIENT_ID` / `MONTA_CLIENT_SECRET` | *required with `ENABLE_MONTA`* | A Monta Hub application's credentials (Profile → Applications) |
| `MONTA_TIMES` | `04:00,21:00` | Local times of day to fetch, comma-separated (it also runs once at startup) |
| `MONTA_LOOKBACK_DAYS` | `0` | Only re-fetch sessions created in the last N days; `0` re-fetches everything every run |
| `MONTA_TABLE` | `monta_charges` | Table the sessions are upserted into (on `id`) |
| `MONTA_VAT_RATE` | `0.21` | VAT included in Monta's `price`, used for `price_excl_vat` |
| `MONTA_RETRY_INTERVAL` | `900` | Seconds between retries after a failed fetch |
| `TZ` | *(container default)* | Timezone for scheduling/logging |

## Log levels

`LOG_LEVEL` sets the minimum severity written to stdout; everything below it is silent. The
default, `WARNING`, is the minimal-logging setting for normal operation:

| Level | Shows | When to use it |
|---|---|---|
| `WARNING` (default) | Config problems, connection loss/refusal, a full write queue, a stale poll skipping an upload, and every caught exception (always logged at error severity with a traceback, regardless of this setting) | Normal operation. Silences the 5-second poll heartbeat and routine push confirmations, so healthy running produces no output at all. |
| `INFO` | The above, plus the recurring poll heartbeat (`Poll ok: ac=...`), startup/connection confirmations, and successful PVOutput/Mindergas pushes | Confirming the container is alive and doing what's expected, without full per-field detail. |
| `DEBUG` | The above, plus per-field detail such as a plug field that failed to parse and was dropped | Diagnosing a specific data problem, e.g. a smart-plug field arriving as something unexpected. |
| `ERROR` | Only config problems and caught exceptions | Quieter than the default; loses the connection-state warnings (MQTT disconnects, a full write queue), which are usually the useful signal before something breaks outright. |
| `CRITICAL` | Practically nothing — this file has no `log.critical` calls | Not useful here; included only because it's one of Python's standard levels. |

An unrecognised value (e.g. a typo) does not stop the container: it falls back to `WARNING` and
prints one line to stderr naming the invalid value and the valid options.

Change it with a redeploy (`docker compose up -d energy-monitor` after editing the compose file's
`LOG_LEVEL`) — there's no way to change it on a running container without restarting it.

## Modbus register map

Targets the **Anker SOLIX Solarbank Max** (model `AE111`), a hybrid unit with its own MPPT inputs
and a battery. The documented registers come from the config in Anker's Home Assistant integration
([anker-charging/ha-anker-solix-official](https://github.com/anker-charging/ha-anker-solix-official))
for this model; a few undocumented ones were identified against the device and are commented as
such in `poll_battery()`. Facts the reads rely on, all verified on the device:

- **32-bit values are big-endian, high word first.** The reverse of many inverters; a low-word-first
  decode gives plausible-looking wrong numbers for small values.
- Addresses are wire addresses (no 1-based offset). Unit id 1; any id other than 0 or 1 gets no
  answer at all, so a wrong id is a timeout, not an exception.
- A read must start on the first word of a value. Inside a block read the firmware returns 0 for
  some undocumented registers, so those (10254, 10156) are read on their own. The string inputs
  (10167–10170) are one small block of their own; the addresses around them are unmapped.
- **Read-only.** 10064 (operating mode), 10071 (power setpoint) and 60000–60003 (charge limits) are
  writable; `_read()` only issues function codes 3 and 4.

| Field | Register | Notes |
|---|---|---|
| `solar.total_active_power` | 10002 + 10004 | PV and third-party PV power, W |
| `solar.total_power_yields` | 10018 (0.1 kWh) + `PV_TOTAL_OFFSET_KWH` | Lifetime PV, kWh |
| `solar.daily_power_yields` | derived | Lifetime total minus the last one before local midnight |
| `solar.mppt_1_voltage`, `mppt_1_current`, `mppt_2_voltage`, `mppt_2_current` | 10167–10170 (0.1 V, 0.01 A, one word each) | The two string inputs. Undocumented; identified on a sunny day: V × A of both strings matched 10002 within about 1 % |
| `solar.total_dc_power` | derived | PV1 V × A + PV2 V × A, W |
| `battery.battery_power` | 10008 | W, positive = discharging |
| `battery.ac_output_power` | 10208 | W the unit delivers into the house |
| `battery.load_power`, `grid_power` | 10010, 10012 | The unit's view from its own CT meter; only as good as that meter's wiring. Not used in any calculation |
| `battery.soc` | 10014 | % |
| `battery.charge/discharge_energy_total` | 10262, 10264 (0.1 kWh) | At the cells; discharge includes standby drain |

The PV rows keep `source=sungrow` from the inverter the unit replaced, for the same reason the
smart-meter rows keep `dsmr`: `source` is a grouping key in the downstream continuous aggregates,
and the daily yield is computed there as the difference of each day's highest lifetime total. With
`PV_TOTAL_OFFSET_KWH` set to the old inverter's final total, the lifetime series simply continues.

## Smart-meter data

Electricity and gas are read **directly off the P1 telegram stream**, not via a third-party
tool's MQTT export, database or REST API. `ENABLE_P1=true` plus `P1_HOST`/`P1_PORT` point this
container at a plain-TCP relay of the meter's P1 port — a ser2net `connection:` block is the
usual case, but anything that forwards the raw serial byte stream over TCP works.

Each telegram is:

1. **Framed and CRC-verified** (`extract_telegram`/`crc16_arc` in `app.py`) — a torn or corrupted
   read is logged and discarded rather than trusted, since a plausible-looking but wrong meter
   reading is worse than a gap.
2. **Parsed by OBIS code** (`parse_p1_telegram`) into electricity fields and, if present, a gas
   reading. The gas M-Bus channel isn't assumed to be a fixed number — it's discovered per telegram
   by scanning for the device-type-003 line, the same way DSMR-reader itself does.

   `_ELEC_OBIS_MAP` is a fixed allowlist; a code not in it is dropped here, before the write, so
   adding a field is a code change rather than a schema one. What's mapped today:

   | OBIS | Column | |
   |---|---|---|
   | `1-0:1.8.1` / `1-0:1.8.2` | `electricity_delivered_1` / `_2` | cumulative import per tariff, kWh |
   | `1-0:2.8.1` / `1-0:2.8.2` | `electricity_returned_1` / `_2` | cumulative export per tariff, kWh |
   | `1-0:1.7.0` / `1-0:2.7.0` | `electricity_currently_delivered` / `_returned` | instantaneous, kW |
   | `1-0:32.7.0` | `phase_voltage_l1` | V |
   | `0-0:96.14.0` | `electricity_tariff` | active band: 1 = low/off-peak, 2 = normal/peak |
   | `1-0:31.7.0` | `phase_power_current_l1` | A, whole amps |

   The last two are integers, not floats (`_ELEC_INT_FIELDS`) — the meter reports them that way and
   they're stored in integer columns. `electricity_tariff` records what the *meter* was registering
   to, which is what actually bills; deriving it from the clock instead goes quietly wrong around
   DST and whenever a supplier changes its schedule. Current is 1 A resolution, so it's for spotting
   a near-limit load rather than fine measurement.

   Adding a field means extending `_ELEC_OBIS_MAP` **and** `TABLE_COLUMNS`, and adding the column to
   the deploying stack's schema first — an image that writes a column the database doesn't have
   fails every insert, whereas a column an older image doesn't know about is simply left NULL.
3. **Cached in memory** for the PVOutput metrics (electricity only — see below) and **written to
   Postgres** (`ELEC_TABLE`/`GAS_TABLE`, default `electricity`/`gas_positions`).

Gas is re-sampled by the meter only every ~5 minutes but repeated in every telegram in between;
`_handle_p1_telegram` skips the write when neither the M-Bus capture timestamp nor the value has
moved, so this doesn't produce one row per second.

**Two things a raw telegram cannot provide**, both deliberate gaps rather than oversights:

- **Day totals.** A third-party tool like DSMR-reader computes these from its own history; the
  telegram itself carries no running-total-since-midnight field. There is no
  `daily_import_wh`/`daily_export_wh`/`daily_house_energy_wh` PVOutput metric here for that reason
  — only the lifetime (`total_*`) and instantaneous (`grid_*`/`house_power_w`) equivalents exist.
- **Gas flow rate.** Only the cumulative `delivered` reading is parsed; a current-flow-rate figure
  would need two readings' timestamps and values, which isn't worth it for a value that changes
  this slowly.

### When the connection stalls without dropping

The reader treats prolonged silence as a dead connection, not as patience. Beyond a certain point
that is the only correct reading of it: telegrams arrive about once a second, so `P1_STALL_TIMEOUT`
(default 180s) of *nothing at all* is not jitter.

This exists because the failure is invisible at the socket layer. A relay can hang while holding
the TCP session open, and a firewall or router reload can quietly drop connection-tracking state
for the flow — in both cases no `FIN` and no `RST` ever arrive. Since this side only ever reads and
never writes, nothing forces the stack to discover the problem, and a `recv()` that merely retries
waits forever. Ingestion stops permanently while the process stays healthy by every measure it
reports.

TCP keepalive is deliberately *not* the mechanism here. Keepalive proves the peer is alive, which a
hung relay would go on doing indefinitely while sending nothing. A stall deadline covers both that
case and a peer that has genuinely vanished, so it replaces keepalive rather than supplementing it.

If `ENABLE_P1_RELAY` is on, this matters twice over: downstream clients are fed from this loop's
own reads, so a stall that isn't recovered takes their feed down too.

Setting `P1_STALL_TIMEOUT=0` restores the old wait-forever behaviour and logs a warning at startup
saying so. There is no good reason to.

### Reading P1 from a dongle's HTTP API

When a Wi-Fi P1 dongle holds the meter's P1 port, there is no telegram stream to connect to. Some
dongles return the latest raw telegram over a local HTTP API instead; HomeWizard's P1 meter does at
`/api/v1/telegram` once its local API is switched on in the HomeWizard app. With `P1_URL` set,
`p1_http_reader_loop` polls that every `P1_POLL_INTERVAL` seconds and treats the response exactly
like a streamed telegram: same CRC check, same parser, same `source` label, and with
`ENABLE_P1_RELAY` the same relay to downstream readers, one whole telegram per new reading.

A response identical to the previous one is the same telegram and is skipped, so polling at the
meter's own rate (once a second for DSMR 5) neither duplicates nor drops readings in practice. The
stall reconnect below doesn't apply, since each poll is a fresh request; `P1_WARN_AFTER` still
reports when no new telegram has arrived for that long, whether the dongle stopped answering or
keeps answering with the same one.

### Sharing the P1 port with something else that also reads it

A P1 port classically accepts only one TCP client, which blocks this container from reading it
directly if something else (DSMR-reader, a display) already has the one connection ser2net
allows.

**The tempting-looking fix does not actually work — learned the hard way in production.** ser2net
(4.x+, YAML config) can define two independent `connection:` blocks that both point at the same
serial device, each on its own TCP port:

```yaml
connection: &con01
    accepter: tcp,2000
    connector: serialdev,/dev/ttyUSB0,115200n81,local

connection: &con02
    accepter: tcp,2001
    connector: serialdev,/dev/ttyUSB0,115200n81,local
```

A quick test of this looked like it worked: both connections read telegrams simultaneously with
no "port already in use" conflict. It doesn't hold up under real, sustained use — ser2net only
lets **one** connection actually hold the underlying device open at a time. A brief test can
coexist with an already-idle-stable connection without ever tripping this, because neither side
needs to *reopen* the device during that short window. The failure only shows up when one side
needs to (re)open the device while the other is already holding it — which a second long-lived
client is guaranteed to eventually cause. In practice: this container's connection (established
once, never dropped) permanently starved the other client's periodic reconnects, which then
crash-looped trying to reopen a device error'ing `Port's device already in use` — a different,
device-level message from the port-level `Port already in use` the plain single-client limit
gives — burning CPU on restart churn and losing telegram capture entirely, not just degrading it.
**Do not use two ser2net connection blocks against the same serial device for two long-lived
clients.**

**The actual fix: relay from this container instead.** `ENABLE_P1_RELAY` re-serves the raw P1
byte stream this container reads to any number of downstream TCP clients on its own port —
`P1_HOST`/`P1_PORT` stays this container's *one* real connection to ser2net (a single, ordinary
`connection:` block, no sharing tricks needed there at all), and whatever else needs the stream
(DSMR-reader, a display) connects to `P1_RELAY_BIND:P1_RELAY_PORT` on this container instead of to
ser2net directly:

| Variable | Default | Description |
|---|---|---|
| `ENABLE_P1_RELAY` | `false` | Re-serve the P1 stream to downstream TCP clients |
| `P1_RELAY_BIND` | `0.0.0.0` | Interface to listen on |
| `P1_RELAY_PORT` | `2000` | Port to listen on (matches DSMR-reader's usual default, so repointing it is often just an IP change) |
| `P1_RELAY_CLIENT_TIMEOUT` | `5` | Seconds a send to a downstream client may block before it's dropped as dead |

Each connected client gets every byte from the moment it connects onward (no replay of what it
missed — the same behavior as connecting to ser2net directly). A downstream client that stops
reading gets a bounded send timeout rather than being allowed to block this container's own
ingestion indefinitely; it's then dropped and closed, freeing it to reconnect fresh.

## Why power_flow updates from two triggers

`power_flow` (solar/grid/house at one instant) was originally only written from the inverter poll
loop, using whatever smart-meter reading happened to be cached at that moment. That left the table
updating only as often as the inverter was polled, even though the smart meter reports far more
frequently — so a Grafana chart drew long straight segments between points, which reads as a stuck
reading rather than a coarse one.

Fixed by writing `power_flow` from **either** side updating: the existing poll-loop trigger, plus a
second trigger from every P1 telegram carrying electricity data (`_handle_p1_telegram` in `app.py`),
each using the other side's latest cached value. During the day, when both sides are updating every
few seconds, this doubles `power_flow`'s write rate — harmless at TimescaleDB's scale, and still
just one row per instant either trigger fires.

### What a stale battery poll means

On the P1 side the battery's contribution comes from `_battery_values_for_derived()`. The unit
answers around the clock (0 W PV at night is a real reading), so a poll older than
`SOLAR_STALE_AFTER` means contact was lost, and the PV and house terms are unknown:

| Last poll | `solar_w` | `house_w` | `grid_w` |
|---|---|---|---|
| fresh | real value | `grid_w` + the unit's AC output | from the meter |
| stale | *omitted* | *omitted* | from the meter |

Omitting matches what `derived_fields()` does for a P1 outage: a gap in Grafana rather than a
plausible-looking zero.

### House use with a battery

`house_w = grid_w + battery_ac_w`, where `battery_ac_w` is the unit's AC output. Before the battery
it was `solar + import − export`; with the battery between the panels and the house, part of the
generation goes into the cells and comes out hours later, so PV + grid would count charging as house
use. The panels reach the house only through the unit, so its AC output is everything the house gets
that isn't from the grid. `battery_ac_w` and `battery_w` (power at the cells) are stored in the
row's `extra` so the formula can be checked afterwards.

One limit, measured: register 10208 only counts output. While the unit charged at 800 W from AC it
stayed 0, so whenever the battery charges from the grid, `house_w` includes that charging power.
In self-consumption mode it charges from the panels only, so this doesn't arise in normal use.
Daily totals downstream can subtract the unit's AC-input counter (10030, `socket_energy_in`).

## Smart plugs (Zigbee2MQTT)

Optionally submeters individual circuits from metering Zigbee smart plugs published by
Zigbee2MQTT onto the MQTT broker this container connects to. Enable with `ENABLE_PLUGS=true` and
list the topics to subscribe to in `PLUG_TOPICS` as `topic=label` pairs; each label becomes the
`source` column value in `PLUG_TABLE` (default `smart_plugs`).

Unlike DSMR there is no fixed schema to map: different plug models publish different extra
fields (e.g. an Aqara plug's `device_temperature`/`power_outage_count` vs. a Tuya plug's
`indicator_mode`). Rather than hand-mapping each model, only the fields common to metering plugs
in general are kept -- `power` (W), `energy` (kWh), `current` (A), `voltage` (V) and `state`
(`ON`/`OFF`) -- so swapping a plug for a different model or adding a new one needs only a
`PLUG_TOPICS` entry, no code change. A topic not listed in `PLUG_TOPICS` is ignored.

## PVOutput mapping

Each PVOutput parameter is assigned a **metric** by name via `PVOUTPUT_V1`…`PVOUTPUT_V6`. Metrics
are already expressed in the unit PVOutput expects, so there is no scaling or formula syntax to
get wrong. Leave a variable unset and that parameter simply isn't uploaded.

The defaults:

```yaml
- PVOUTPUT_V1=daily_generation_wh    # Energy Generation  (Wh)
- PVOUTPUT_V2=generation_w           # Power Generation   (W)
# V3 (Energy Consumption), V4 (Power Consumption), V5 (Temperature) and V6 (Voltage) unset.
# grid_voltage_v is the usual V6; it needs ENABLE_P1.
```

### Available metrics

| Metric | Unit | Source | Notes |
|---|---|---|---|
| `daily_generation_wh` | Wh | battery | Day total: lifetime total minus the last one before midnight. Omitted until a baseline exists |
| `total_generation_wh` | Wh | battery | Lifetime counter (including `PV_TOTAL_OFFSET_KWH`) |
| `generation_w` | W | battery | Current PV power |
| `battery_soc` | % | battery | State of charge |
| `grid_import_w` | W | meter | Currently drawn from the grid |
| `grid_export_w` | W | meter | Currently fed back |
| `grid_voltage_v` | V | meter | Voltage as the smart meter measures it |
| `total_import_wh` | Wh | meter | Lifetime, both tariffs summed |
| `total_export_wh` | Wh | meter | Lifetime, both tariffs summed |
| `house_power_w` | W | derived | `import − export + battery AC output` — what the house actually uses |

There's no day-total equivalent of `house_power_w`/`grid_import_w` (a `daily_import_wh` or
similar) — the raw P1 telegram carries no running-total-since-midnight field, only cumulative
lifetime and instantaneous values; see [Smart-meter data](#smart-meter-data).

Metrics sourced from the **meter** or **derived** from it read the cached P1 electricity reading
(`get_elec_values()` in `app.py`). No Postgres query is involved, so these stay usable even while
Postgres writes are failing.

If no electricity reading has arrived yet, or the cached one is older than `P1_MAX_DATA_AGE`, the
parameters needing it are dropped and the upload still goes out with the battery-only values. A
stalled P1 connection therefore degrades the upload rather than losing it or reporting figures
from hours ago.

### Reporting consumption as well as generation

```yaml
- PVOUTPUT_V4=house_power_w
```

There is no lifetime house-energy metric any more: it would need a lifetime counter of the unit's
AC output, and none has been identified. PVOutput derives consumption energy from `v4` itself.

`PVOUTPUT_CUMULATIVE_FLAG` (PVOutput's `c1`) **must agree** with whether `v1`/`v3` are lifetime
counters — `1` = both lifetime, `2` = only `v1`, `3` = only `v3`, omitted = both are day totals. A
mismatch makes PVOutput derive the wrong energy figures, so the container checks the combination at
startup and logs a warning describing the fix. An unknown metric name is a hard error: it logs the
full list of valid names and exits rather than uploading something unintended.

## Stroohm / Monta charging sessions

With `ENABLE_MONTA=true` this container fetches your EV charging sessions from Monta's Public API
(a Stroohm charge card runs on Monta) at startup and then every day at each of `MONTA_TIMES`
(default 04:00 and 21:00: the evening run shows an early-evening session the same night, the
morning one catches later ones). Every completed session is upserted into `MONTA_TABLE` on Monta's
session id, in one transaction, so re-fetching the whole history each run is harmless and picks up
sessions Monta corrects afterwards.

The API client is [stroohm-monta-api](https://github.com/puijken/stroohm-monta-api), which the
image build downloads at a pinned commit (`STROOHM_MONTA_API_REF` in the Dockerfile). Its README
covers creating the credentials (Monta Hub → Profile → Applications) and what the fields mean,
e.g. that `price_incl_vat` is what you paid and that sessions carry a charger ID but no address.

The table must exist first:

```sql
CREATE TABLE monta_charges (
    id bigint PRIMARY KEY, human_readable_id text, state text, charge_point_id bigint,
    started_at timestamptz NOT NULL, stopped_at timestamptz, energy_kwh double precision NOT NULL,
    price_incl_vat double precision, price_excl_vat double precision, currency text,
    raw jsonb, fetched_at timestamptz NOT NULL DEFAULT now()
);
```

## Mindergas integration

Modelled on DSMR-reader's own exporter
([src/dsmr_mindergas/services.py](https://github.com/dsmrreader/dsmr-reader/blob/v6/src/dsmr_mindergas/services.py)):
picks the last gas reading timestamped strictly before local midnight (i.e. the previous day's
final reading) and POSTs `{"date": <that day>, "reading": "<m3>"}` to
`https://www.mindergas.nl/api/meter_readings` with an `AUTH-TOKEN` header.

One deliberate difference: DSMR-reader constrains that lookup to a fixed window (the 3 hours before
midnight) and gives up if no reading landed in it. `get_last_gas_reading()` applies **no lower
bound** — Mindergas wants the day's actual final reading, so a gap in gas ingestion should make the
upload late-but-correct rather than skip the day entirely. The meter reports only every 5 minutes
and `delivered` is a monotonic counter, so the most recent reading before midnight is the right
answer however far back it turns out to be.

The upload fires at `MINDERGAS_HOUR:MINDERGAS_MINUTE` (default 00:05 local), inside the 00:05–01:00
window Mindergas asks uploaders to use so their server load stays spread out.

Unlike the PVOutput metrics this reads from **Postgres**, not an in-memory cache — it needs the
value as it stood before midnight, which requires timestamped history rather than the latest
value. It reads the gas data this container itself wrote from the P1 telegram stream
(`MINDERGAS_GAS_TABLE` defaults to `GAS_TABLE`), so the two stay consistent automatically.

Timestamps are timezone-aware on purpose: Postgres stores `timestamptz` as UTC and interprets a
naive value as the session's local time, so passing naive local time here would be ambiguous.

## Tests

`smoke_test.py` exercises every external dependency this container talks to — Modbus, MQTT and
Postgres — without a real battery, broker or database. It started out covering only the register
decode path and has grown alongside the features above:

- **Modbus decode**: an in-process Modbus server serves the Solarbank's registers in its own layout
  (high word first, negative values, registers read on their own, a holding register), polled
  through `app.poll_battery`; asserts every decoded value and that PV covers charging plus AC
  output — catching word-order and scaling regressions. `_read()` must refuse write function codes.
- **MQTT client + the `ENABLE_MQTT_PUBLISH` switch**: `app.mqtt_connect()` still returns a working
  client, and publishing toggles on/off correctly without disabling plug ingestion, which shares
  the same broker connection.
- **Postgres write and read paths**: `app.write_postgres()` / `app.get_last_gas_reading()` against
  a fake capturing connection — no real database needed, but the SQL/params/`ON CONFLICT` clause
  are checked exactly.
- **Postgres write retry**: `app._write_until_done()` keeps a point through a connection-level
  failure (backoff, fresh connection each attempt) and drops a point the database rejects without
  retrying it.
- **Clean stop**: the real `app.py` run as a subprocess must exit 0 within seconds of a `SIGTERM`,
  and `app.drain_write_queue()` must wait for queued and in-flight points, stay bounded when
  nothing is writing, and not be thrown off by a point evicted from a full queue. The process is
  PID 1 in its container, where an unhandled `SIGTERM` is ignored: every `docker stop` used to
  wait out its grace period and then kill it.
- **P1 telegram ingestion**: framing and CRC verification across split reads and a corrupted
  telegram, OBIS parsing into electricity/gas readings, and a connection that stays open but stops
  delivering — `p1_reader_loop` must tear it down and rebuild it rather than waiting on it forever.
- **DSMR timestamps**: the `S`/`W` suffix resolving the autumn DST fall-back hour to two distinct
  UTC instants, rather than the two passes colliding on one primary key.
- **P1 relay fan-out**: a healthy downstream client keeps receiving bytes; one whose `sendall()`
  raises is dropped and closed rather than taking down the broadcast.
- **Smart plugs**: real captured Zigbee2MQTT payloads from both plug models on this network (Aqara,
  Tuya), checking that only the common fields are kept and per-model extras are dropped.
- **PVOutput metrics**: the `app.METRICS` formulas, and that a missing/stale electricity reading
  degrades the upload to battery-only values rather than losing it; `house_w = grid + battery AC
  output` in `power_flow`; a stale battery poll omitted rather than zeroed; the daily yield from the
  lifetime total and the midnight baseline.
- **Smart-meter `source` stability**: `P1_SOURCE` stays pinned to `"dsmr"`, since changing it splits
  every downstream continuous aggregate at the cutover instant.

```bash
docker build -t energy-monitor:dev .
docker run --rm -v "$PWD/smoke_test.py:/app/smoke_test.py:ro" energy-monitor:dev python3 /app/smoke_test.py
```

Both workflows run it: `pr-validate.yml` on pull requests, and `build.yml` **before** pushing the
image, so a dependency bump that breaks any of the above at runtime can never reach `:latest`. That
matters because building alone proves nothing here — pymodbus 3.14 removed the `slave=` keyword in
favour of `device_id=`, which built cleanly and failed on every poll. The app now resolves that
argument name from the function signature, and the test would catch the next such change.

Note that Dependabot auto-merges anything it doesn't classify as `semver-major`, and it only waits
for status checks that branch protection marks **required**. Without branch protection on `main`,
`pr-validate` will not block a merge — the pre-push check in `build.yml` is what actually protects
the published image.

## Docker Compose

See `docker-compose.yml` in this repo for a usage example. It is meant to sit alongside the
TimescaleDB instance it writes to and whatever Grafana (or other client) reads that database.
