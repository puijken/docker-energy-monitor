# Energy Monitor

Collects whole-house energy data into one TimescaleDB (Postgres): solar generation read from a
Sungrow inverter over Modbus TCP, and electricity/gas consumption read directly off the P1 smart
meter's telegram stream over the network. Optionally re-publishes solar values to MQTT, and uploads
to PVOutput (every few minutes) and Mindergas (once daily). Built as a self-maintained replacement
for the third-party `bohdans/sungather` image.

## Features

- Polls the inverter every `SCAN_INTERVAL` seconds over Modbus TCP (function code 4 / input
  registers): generation power, daily and lifetime yield, DC/MPPT voltage & current, phase voltage,
  internal temperature.
- Connects to a plain-TCP relay of the meter's P1 port and parses DSMR telegrams itself
  (CRC-verified) for smart-meter electricity and gas readings, and writes those to the same
  database too — so one place holds generation *and* consumption, with no other project's exporter
  in the path.
- Writes every reading to TimescaleDB (`solar` table, `source=sungrow`; smart-meter data under
  `electricity` / `gas_positions` with `source=dsmr`). That `source` value names the *meter*, not
  the program reading it, so it stays put across ingestion-path changes — it is a grouping key in
  downstream continuous aggregates, and changing it starts a parallel series instead of continuing
  the existing one. Plug fields not in a table's fixed columns land in an `extra` JSONB column
  instead of failing the write, so a new plug field needs no code change here — see
  [Smart plugs](#smart-plugs-zigbee2mqtt). Electricity is *not* schemaless in the same way: OBIS
  codes are mapped through a fixed allowlist and anything unlisted is dropped at parse time, so a
  new smart-meter field does need one.
- Optionally re-publishes readings to MQTT, one retained topic per field, under two separate
  prefixes: inverter data under `MQTT_TOPIC_PREFIX` and smart-meter electricity under
  `MQTT_ELEC_TOPIC_PREFIX`, so a subscriber can take either side alone. Both are gated by the one
  `ENABLE_MQTT_PUBLISH` switch (default off). Outbound only — nothing here reads it back, and
  Postgres is written either way.
- Optionally pushes a status update to PVOutput on an interval (gated by `ENABLE_PVOUTPUT`,
  default off).
- Optionally pushes the previous day's cumulative gas meter reading to Mindergas shortly after
  midnight (gated by `ENABLE_MINDERGAS`, default off), reading the value back out of the
  `gas_positions` table this container itself writes.

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
| `INVERTER_HOST` | *required* | Inverter/dongle IP address |
| `INVERTER_PORT` | `502` | Modbus TCP port |
| `INVERTER_SLAVE_ID` | `1` | Modbus slave/unit ID |
| `SCAN_INTERVAL` | `15` | Seconds between inverter polls |
| `POSTGRES_HOST` | *required* | TimescaleDB/Postgres hostname, e.g. `timescaledb` |
| `POSTGRES_PORT` | `5432` | Postgres port |
| `POSTGRES_DB` | `energy` | Database name |
| `POSTGRES_USER` | *required* | Database user |
| `POSTGRES_PASSWORD` | *required* | Database password |
| `POSTGRES_TABLE` | `solar` | Table name written to for inverter readings |
| `WRITE_QUEUE_SIZE` | `2000` | Max points buffered for the Postgres writer thread. A prolonged outage drops the oldest queued point first rather than growing without bound. |
| `MQTT_HOST` | *required if MQTT is used* | Broker hostname/IP. Only required when `ENABLE_MQTT_PUBLISH` or `ENABLE_PLUGS` is on; with both off the container never connects to a broker at all. |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USERNAME` | *(none)* | MQTT username, if required |
| `MQTT_PASSWORD` | *(none)* | MQTT password, if required |
| `MQTT_WARN_AFTER` | `60` | Seconds without a broker connection before warning it looks unreachable; repeats every interval while still disconnected |
| `ENABLE_MQTT_PUBLISH` | `false` | Re-publish each inverter reading to MQTT, one retained topic per field. Purely an outbound copy — Postgres is written either way and nothing here reads it back. **Changed from `true` to `false` on 2026-08-14**: if you are upgrading and something subscribes to these topics, set this to `true` or it goes quiet. Does **not** affect plug ingestion, which shares the same connection. |
| `MQTT_TOPIC_PREFIX` | `energy/solar` | Topic prefix for **inverter** readings; one sub-topic per field. |
| `MQTT_ELEC_TOPIC_PREFIX` | `energy/electricity` | Topic prefix for **smart-meter electricity** readings, published at `P1_MIN_INTERVAL` cadence. Only produces anything when `ENABLE_P1` is on. Gas and plug data are not published. |
| `MQTT_PUBLISH_FIELDS` | *(all)* | Comma-separated **inverter** field names to publish, e.g. `total_active_power,run_state`. Leave unset to publish everything. Applies to the inverter topic tree only, not `MQTT_ELEC_TOPIC_PREFIX`. These are the field names as published, which are not quite the register names: use `run_state`, not the raw `work_state_1` it is decoded from, and `total_power_yields_precise` is available even though it is not part of the main register block. Unrecognised names are warned about at startup. |
| `ENABLE_P1` | `false` | Connect to a P1 telegram relay for smart-meter data. See [Smart-meter data](#smart-meter-data). |
| `P1_HOST` | *required if `ENABLE_P1=true`* | Host/IP of the P1 telegram relay |
| `P1_PORT` | `2001` | Port of the P1 telegram relay |
| `ELEC_TABLE` | `electricity` | Table for electricity readings |
| `GAS_TABLE` | `gas_positions` | Table for gas readings |
| `P1_WARN_AFTER` | `60` | Seconds without a telegram before warning the connection looks stalled |
| `P1_STALL_TIMEOUT` | `180` | Seconds without a telegram before the connection is torn down and rebuilt, however healthy the socket still looks. `0` disables the reconnect — see [When the connection stalls without dropping](#when-the-connection-stalls-without-dropping) |
| `P1_MAX_DATA_AGE` | `300` | Cached electricity values older than this count as absent |
| `P1_MIN_INTERVAL` | `SCAN_INTERVAL` | Minimum seconds between Postgres writes of electricity data (telegrams arrive roughly once a second); the in-memory cache still updates every telegram regardless, so PVOutput/`power_flow` always see the latest reading |
| `ENABLE_P1_RELAY` | `false` | Re-serve the P1 stream to downstream TCP clients (e.g. DSMR-reader). **Never share ser2net itself between two long-lived clients — use this instead.** See [Sharing the P1 port](#sharing-the-p1-port-with-something-else-that-also-reads-it). |
| `P1_RELAY_BIND` / `P1_RELAY_PORT` | `0.0.0.0` / `2000` | Interface/port the relay listens on |
| `P1_RELAY_CLIENT_TIMEOUT` | `5` | Seconds a send to a downstream relay client may block before it's dropped as dead |
| `ENABLE_DERIVED` | `true` | Write `power_flow` (solar/grid/house at one instant), combining the latest cached inverter and P1 electricity readings whenever either updates |
| `DERIVED_TABLE` | `power_flow` | Table written to |
| `SOLAR_STALE_AFTER` | `120` | Seconds after which an inverter poll is too old to reuse for `power_flow`. What happens past it depends on the inverter's last known `run_state` — see [What a stale inverter poll means](#what-a-stale-inverter-poll-means). |
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

Targets the **Sungrow SG5.0RS** (`device_type_code` `0x2606`, nominal 5.0 kW) — a single-phase
residential string inverter, so the hybrid-only (SH-series) register blocks don't apply. Register
addresses, datatypes and scaling are Sungrow's own published Modbus protocol definition as encoded
in [bohdan-s/SungrowClient](https://github.com/bohdan-s/SungrowClient) — referenced for
interoperability, not vendored code. See `REGISTERS` in `app.py` for the exact addresses.

Addresses there are 1-based protocol register numbers; Modbus reads are 0-based, hence the `-1`
in `poll_inverter`. All registers are read in a single input-register (function code 4) request.

`run_state` is decoded from the real `work_state_1` register (address 5038, outside the main block
but still within one Modbus read) via `WORK_STATES` in `app.py` — `Run`, `Stop`, `Standby`, `Fault`,
etc. An earlier assumption that this inverter family exposed no run/system-state register (that
block being hybrid-only in Sungrow's protocol) turned out to be wrong; it was derived from
`total_active_power > 0` instead until this was corrected. An unmapped code is logged as
`Unknown (0x....)` rather than silently dropped, so a new one shows up rather than going missing.

Values were validated against the real inverter: MPPT1 + MPPT2 DC power sums to `total_dc_power`,
and `total_active_power` (AC) sits just below it with a realistic conversion loss.

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

### What a stale inverter poll means

On the P1 side the inverter's contribution comes from `_solar_power_for_derived()`, and a poll older
than `SOLAR_STALE_AFTER` is genuinely ambiguous: it can mean the inverter stopped producing, or that
contact was lost with one that is producing fine. Those call for opposite answers.

This used to always answer 0 W, on the grounds that a stale poll means "asleep, not producing". That
reasoning assumed the inverter drops off the network when it stops generating. If yours does not —
this one stays reachable around the clock and reports `Standby` at exactly 0 W all night — then
nights never relied on the substitution at all, because that 0 W is real data arriving through the
normal path. The substitution then only ever fired on a genuine poll failure, which is precisely
when 0 W is wrong: during a daytime dropout it reports a producing array as idle, and since
`house = solar + import − export`, an exporting house computes as **negative** consumption.

Resolved from the inverter's own last-known `run_state` instead of guessing:

| Last poll | Last known state | `solar_w` | `house_w` | `grid_w` |
|---|---|---|---|---|
| fresh | anything | real value | computed | from the meter |
| stale | idle (`IDLE_RUN_STATES`) | `0` — a real zero | computed | from the meter |
| stale | producing, or unmapped | *omitted* | *omitted* | from the meter |

Omitting matches what `derived_fields()` already does for a P1 outage: a gap in Grafana rather than
a plausible-looking zero. `grid_w` comes straight off the meter, so it stays continuous regardless
of what the inverter is doing.

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

The defaults reproduce exactly what the previous SunGather setup sent:

```yaml
- PVOUTPUT_V1=daily_generation_wh    # Energy Generation  (Wh)
- PVOUTPUT_V2=generation_w           # Power Generation   (W)
- PVOUTPUT_V6=inverter_voltage_v     # Voltage            (V)
# V3 (Energy Consumption), V4 (Power Consumption) and V5 (Temperature) unset
```

### Available metrics

| Metric | Unit | Source | Notes |
|---|---|---|---|
| `daily_generation_wh` | Wh | inverter | Day total, resets at midnight |
| `total_generation_wh` | Wh | inverter | Lifetime counter |
| `generation_w` | W | inverter | Current AC output |
| `dc_power_w` | W | inverter | Combined MPPT DC input |
| `inverter_voltage_v` | V | inverter | Grid voltage as the inverter measures it |
| `inverter_temp_c` | °C | inverter | **Heatsink**, not ambient (~50 °C running) — mapping this to `v5` will skew PVOutput's insolation figures, which is why the old setup left it off |
| `grid_import_w` | W | meter | Currently drawn from the grid |
| `grid_export_w` | W | meter | Currently fed back |
| `grid_voltage_v` | V | meter | Voltage as the smart meter measures it |
| `total_import_wh` | Wh | meter | Lifetime, both tariffs summed |
| `total_export_wh` | Wh | meter | Lifetime, both tariffs summed |
| `house_power_w` | W | derived | `generation + import − export` — what the house actually uses |
| `total_house_energy_wh` | Wh | derived | Lifetime equivalent of the above |

There's no day-total equivalent of `house_power_w`/`grid_import_w` (a `daily_import_wh` or
similar) — the raw P1 telegram carries no running-total-since-midnight field, only cumulative
lifetime and instantaneous values; see [Smart-meter data](#smart-meter-data).

Metrics sourced from the **meter** or **derived** from it read the cached P1 electricity reading
(`get_elec_values()` in `app.py`). No Postgres query is involved, so these stay usable even while
Postgres writes are failing.

If no electricity reading has arrived yet, or the cached one is older than `P1_MAX_DATA_AGE`, the
parameters needing it are dropped and the upload still goes out with the inverter-only values. A
stalled P1 connection therefore degrades the upload rather than losing it or reporting figures
from hours ago.

### Reporting consumption as well as generation

To have PVOutput show household consumption, not just generation:

```yaml
- PVOUTPUT_V1=total_generation_wh
- PVOUTPUT_V3=total_house_energy_wh
- PVOUTPUT_V4=house_power_w
- PVOUTPUT_CUMULATIVE_FLAG=1
```

Lifetime metrics are used for `v1`/`v3` here on purpose: lifetime counters never reset, so there's
no day-boundary or timezone ambiguity about which day a figure belongs to.

`PVOUTPUT_CUMULATIVE_FLAG` (PVOutput's `c1`) **must agree** with whether `v1`/`v3` are lifetime
counters — `1` = both lifetime, `2` = only `v1`, `3` = only `v3`, omitted = both are day totals. A
mismatch makes PVOutput derive the wrong energy figures, so the container checks the combination at
startup and logs a warning describing the fix. An unknown metric name is a hard error: it logs the
full list of valid names and exits rather than uploading something unintended.

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
Postgres — without a real inverter, broker or database. It started out covering only the register
decode path and has grown alongside the features above:

- **Modbus decode**: an in-process Modbus server serves known register values, polled through
  `app.poll_inverter`; asserts the decoded results, that DC input covers AC output, and that the
  two MPPT strings sum to `total_dc_power` — catching word-order and scaling regressions.
- **MQTT client + the `ENABLE_MQTT_PUBLISH` switch**: `app.mqtt_connect()` still returns a working
  client, and publishing toggles on/off correctly without disabling plug ingestion, which shares
  the same broker connection.
- **Postgres write and read paths**: `app.write_postgres()` / `app.get_last_gas_reading()` against
  a fake capturing connection — no real database needed, but the SQL/params/`ON CONFLICT` clause
  are checked exactly.
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
  degrades the upload to inverter-only values rather than losing it.
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
