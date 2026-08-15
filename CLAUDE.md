# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-file Python service (`app.py`) that polls a Sungrow inverter over Modbus TCP, reads
electricity/gas readings directly off the P1 smart-meter telegram stream over the network, and
consumes Zigbee2MQTT for per-circuit smart-plug submetering; writes everything to TimescaleDB,
optionally republishes inverter and smart-meter readings to MQTT (gated by `ENABLE_MQTT_PUBLISH`,
default off), and optionally uploads to PVOutput and Mindergas. Built as a self-maintained
replacement for the third-party `bohdans/sungather` image. Published to
`ghcr.io/puijken/energy-monitor`.

**This repo is public.** Real device names, IPs, and deployment status belong in a gitignored
`*.local.md` file (see `DEPLOYMENT.local.md`), never in README.md, code comments, or committed
compose files.

Read `README.md` before making non-trivial changes — it documents *why*, in detail, for nearly
every subsystem below (register map validation, the PVOutput `c1`/lifetime-metric interaction, the
P1 telegram framing/CRC and OBIS mapping, sharing one P1 port across two readers, etc.). This file
only covers what the README doesn't: where things live in `app.py` and how to build/test.

## Commands

```bash
# Build
docker build -t energy-monitor:dev .

# Run the smoke test (in-process Modbus server + fake MQTT/Postgres, asserts decoded register values)
docker run --rm -v "$PWD/smoke_test.py:/app/smoke_test.py:ro" energy-monitor:dev python3 /app/smoke_test.py
# or, with deps installed locally:
python3 smoke_test.py

# Run one check instead of all 15 (the full run takes ~10s; check_p1_stall_reconnect alone sleeps
# several seconds against a real socket). Check functions take a `failures` list and append to it
# rather than raising, so an empty list means it passed:
docker run --rm -v "$PWD/smoke_test.py:/app/smoke_test.py:ro" energy-monitor:dev \
  python3 -c "import smoke_test; f=[]; smoke_test.check_dsmr_timestamp_parsing(f); print(f or 'ok')"
# List them: grep '^def check_' smoke_test.py
#
# Checks happen not to depend on each other today, but nothing enforces that -- several mutate
# module globals in app (write throttles, caches, PLUG_TOPIC_MAP) and must restore them in a
# finally block. If one passes alone but fails in the suite, suspect leaked state from an earlier
# check rather than the check itself.

# Run standalone (needs a real or reachable inverter/broker/DB, or edit docker-compose.yml)
docker compose up -d
docker compose logs -f energy-monitor
```

There's no linter configured and no test framework beyond `smoke_test.py` — it's a single
self-contained script with hand-rolled assertions, not pytest. Both CI workflows
(`pr-validate.yml`, `build.yml`) run it; `build.yml` runs it **before** pushing to `:latest`, which
is the only thing that actually gates the published image (see Tests below).

## Architecture

Everything lives in `app.py` as one process with in-memory shared state — no framework, no
classes, just module-level globals guarded by `_state_lock` (or their own dedicated lock, see
`_elec_lock`) and daemon threads started from `main()`:

- **Main thread** — the inverter poll loop (`SCAN_INTERVAL` seconds): connects Modbus, calls
  `poll_inverter`, updates `latest_values`/`latest_poll_monotonic` under the lock, queues a
  Postgres write, computes `derived_fields` (power_flow), and publishes to MQTT if
  `ENABLE_MQTT_PUBLISH`. Each sink is wrapped
  in its own `try/except` so a Postgres or MQTT outage can't stop the others.
- **`pg_writer_loop`** — drains a `queue.Queue` fed by `queue_write()` and performs the actual
  Postgres inserts, so DB latency never stretches the poll cadence.
- **`mqtt_watchdog_loop`** — the paho-mqtt client runs its own network thread for the pub/sub
  connection; this loop separately reconnects on disconnect (`_on_mqtt_disconnect` sets a flag,
  this polls it) and resubscribes. Smart-meter data doesn't go through the broker at all. Both
  directions share this one client, so `MQTT_ENABLED` (= `ENABLE_MQTT_PUBLISH or ENABLE_PLUGS`)
  decides whether `main()` connects at all — with both off there's no client, no watchdog, and
  `MQTT_HOST` stops being required.
- **`p1_reader_loop`** (when `ENABLE_P1=true`) — owns a persistent TCP connection to the P1 relay,
  buffers bytes, calls `extract_telegram` to pull out complete CRC-verified telegrams, and hands
  each to `_handle_p1_telegram`. Reconnects with exponential backoff (1s → 30s) on any failure;
  logs once (not per-telegram) if nothing arrives for `P1_WARN_AFTER` seconds, and **tears the
  connection down and rebuilds it after `P1_STALL_TIMEOUT`** of total silence. That last part is
  load-bearing, not defensive tidying: a `recv()` timeout used to `continue`, so an upstream that
  hung while holding the session open (no FIN, no RST — and this side never writes, so nothing
  forces the stack to notice) stalled ingestion permanently behind a single warning. Backoff resets
  on *data*, not on a successful connect, so a connect-then-silence upstream still backs off.
  Deliberately not TCP keepalive — that only proves the peer is alive, which a hung relay keeps
  doing while sending nothing. **Must be the only direct client of the upstream relay/ser2net** —
  see the `ENABLE_P1_RELAY` note below and its much longer comment in `app.py` before changing this.
- **`p1_relay_accept_loop`** (when `ENABLE_P1_RELAY=true`) — accepts downstream TCP clients on
  `P1_RELAY_BIND:P1_RELAY_PORT` and adds each to `_relay_clients`. `p1_reader_loop`'s own recv loop
  is what actually writes to them (`_relay_broadcast`, called on every chunk read from upstream) —
  this exists so a second consumer (DSMR-reader) can get the P1 stream from *this* container
  instead of opening a second connection to ser2net itself, which does not actually grant
  concurrent access to one serial device despite briefly appearing to (crash-looped DSMR-reader
  and spiked its CPU in production before this was built — full incident in
  `DEPLOYMENT.local.md`). A downstream client is dropped+closed if a send to it ever blocks past
  `P1_RELAY_CLIENT_TIMEOUT`, so a stuck client can't stall this container's own ingestion.
- **`pvoutput_loop`** — fires on wall-clock-aligned `PVOUTPUT_INTERVAL` boundaries (:00/:05/:10…),
  not a timer from container start, so a restart can't shift upload times.
- **`mindergas_loop`** — fires once daily at `MINDERGAS_HOUR:MINDERGAS_MINUTE`, retrying within
  that same hour on failure.

`_handle_p1_telegram` runs on `p1_reader_loop`'s own thread, which is the *other* trigger for
`power_flow` writes (see README's "Why power_flow updates from two triggers") — so `_state_lock`
covers cross-thread reads of `latest_values`/`latest_poll_monotonic` from both the poll loop and
that thread, `_elec_lock` covers `_elec_cache` (the cached electricity reading PVOutput/
`derived_fields` read back via `get_elec_values()`), and `_relay_lock` covers `_relay_clients`
(written by `p1_relay_accept_loop` on new connections, read/pruned by `p1_reader_loop`'s
`_relay_broadcast` calls).

Key structures/functions to know before editing:

- `REGISTERS` — the Modbus register map: address, datatype, scaling per field. Addresses are
  Sungrow's 1-based protocol numbers; `poll_inverter` does the `-1` for 0-based Modbus reads.
- `decode_register` — **32-bit values are low-word-first.** A big-endian read of the same register
  produces plausible-looking wrong numbers, not an error — this is what `smoke_test.py`'s MPPT-sum
  and AC/DC-ratio assertions exist to catch.
- `WORK_STATES` — maps `work_state_1` (register 5038) to human states; unmapped codes log as
  `Unknown (0x....)` rather than being silently dropped.
- `publish_mqtt` — takes the prefix and an optional field allowlist, so one function serves both
  topic trees. Called from *two* threads: the poll loop (inverter, `MQTT_TOPIC_PREFIX`) and
  `_handle_p1_telegram` (electricity, `MQTT_ELEC_TOPIC_PREFIX`, on the same throttle as the
  Postgres write). The latter reaches the client via the module-level `_mqtt_client`, set once by
  `main()`; paho's `publish()` is thread-safe so no lock is involved.
- `crc16_arc` / `extract_telegram` — CRC-16/ARC verification and telegram framing over a raw byte
  stream. `extract_telegram` is called in a loop that keeps draining the buffer as long as it's
  shrinking (a successful parse *and* a discarded CRC failure both still consume bytes), which is
  what lets a torn read or a mid-telegram connection start reassemble correctly instead of wedging.
- `parse_p1_telegram` — OBIS code → electricity fields + optional gas reading. The gas M-Bus
  channel is discovered per telegram (device-type-003 line), never assumed fixed. `_ELEC_OBIS_MAP`
  is a hard allowlist: unlisted codes are dropped here, *before* `write_postgres`, so unlike plug
  data they never reach the `extra` JSONB column and adding one is a code change. Adding a field
  means `_ELEC_OBIS_MAP` **and** `TABLE_COLUMNS`, plus the column in the deploying stack's schema
  **first** — a new image writing a column the DB lacks fails every insert; an older image ignoring
  a column it doesn't know about is harmless. `_ELEC_INT_FIELDS` marks the ones the meter reports
  as whole numbers (`electricity_tariff`, `phase_power_current_l1`), cast to `int` so they land in
  integer columns; `2 == 2.0` in Python, so the smoke test asserts the *type* separately.
- `_parse_dsmr_timestamp` — the S/W suffix is load-bearing, not decoration: it maps to `fold`, and
  it is the only thing separating the two passes of the autumn DST fall-back hour. Getting this
  wrong doesn't error, it silently overwrites an hour of readings once a year via the write path's
  `ON CONFLICT`. `smoke_test.py` asserts against real UTC instants for exactly this reason.
- `P1_SOURCE` — the `source` column value for smart-meter rows, pinned to `"dsmr"` and asserted in
  `smoke_test.py`. It is a GROUP BY key in the deploying stack's continuous aggregates, so changing
  it splits history rather than continuing it. Read its comment before touching it.
- `derived_fields` / `_solar_power_for_derived` — compute `power_flow` (solar/grid/house at one
  instant) from independently-timestamped inverter and P1 electricity readings. A stale inverter
  poll is resolved from its last known `run_state`, NOT by substituting 0: idle (`IDLE_RUN_STATES`)
  is a real zero, anything else returns None and the caller omits `solar_w`/`house_w`. It used to
  always substitute 0, justified by the inverter going offline overnight — it doesn't, it reports
  `Standby` at 0 W around the clock, so that only ever fired on real poll failures and turned
  `house_w` negative. A P1 gap likewise omits grid/house rather than guessing.
- `build_pvoutput_params` / `push_pvoutput` — maps `PVOUTPUT_V1`…`V6` metric names to values;
  validates the `PVOUTPUT_CUMULATIVE_FLAG`/lifetime-metric combination at startup (see README's
  PVOutput mapping section before touching this).
- `get_last_gas_reading` / `push_mindergas` — reads gas history back out of Postgres (not the
  in-memory cache, unlike PVOutput) to find the last reading strictly before local midnight.
- `TABLE_COLUMNS` / `write_postgres` — a field not listed for its table lands in that table's
  `extra` JSONB column instead of failing the insert. That makes **plug** data genuinely schemaless:
  a new Zigbee2MQTT field needs no code change here. It does **not** extend to smart-meter fields,
  even though both go through the same write path — `parse_p1_telegram` has already filtered those
  through the `_ELEC_OBIS_MAP` allowlist, so unlisted OBIS codes never reach `write_postgres` at all
  and `electricity.extra` is always NULL.
- `validate_config()` — startup checks (env combinations, PVOutput flag/metric agreement, that
  `ENABLE_MINDERGAS` has `ENABLE_P1` to actually feed it); prefer extending this over letting a bad
  config fail loudly at run time later.

## Tests

`smoke_test.py` spins up an in-process Modbus TCP server with known register values, polls it
through the real `poll_inverter`, and asserts decoded values match expected, DC input covers AC
output, and the two MPPT strings sum to `total_dc_power`. It has grown well past Modbus alone:
MQTT client construction and the `ENABLE_MQTT_PUBLISH` switch (publishing toggles without
disabling plug ingestion, which shares the same connection), the Postgres write/read paths against
a fake connection, P1 telegram framing/CRC/OBIS parsing plus the stall-reconnect and DST
fall-back-hour timestamp handling, the P1 relay's fan-out, Zigbee2MQTT plug payloads from both
models on this network, the PVOutput metric formulas, the derived `power_flow` fields and solar
staleness fallback, and that `P1_SOURCE` stays pinned to `dsmr` are all covered too — see
`smoke_test.py`'s own module docstring and final summary line for the complete list. The original
Modbus check is what caught a pymodbus 3.14 change (`slave=` → `device_id=` kwarg) that built fine
and failed on every poll in production — building alone proves nothing, hence the smoke test being
a required pre-push step in `build.yml`.

Dependabot auto-merges non-major bumps, and without branch protection on `main`, `pr-validate.yml`
does not block a merge on its own — `build.yml`'s pre-push smoke test run is what actually
protects `:latest`.
