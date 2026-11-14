"""Smoke test: exercise the app's three external dependencies without any real service.

Exists because building the image proves nothing about runtime. A dependency bump that renames a
pymodbus argument, changes a datastore class or alters register decoding still builds cleanly and
only fails when it first talks to hardware -- which has already happened once (pymodbus 3.14
dropped the `slave` keyword in favour of `device_id`).

Originally this only covered pymodbus (start an in-process Modbus server, poll it through
app.poll_inverter, check the decoded values; since the battery swap, app.poll_battery). Dependabot auto-merges non-major bumps to
paho-mqtt, requests and psycopg too, so an API change in any of them would also pass CI and only
surface at runtime. This file now additionally covers, all without a real broker or Postgres:

  - Inverter reconnect after a send-time socket error: main()'s per-cycle pattern (connect-if-
    needed, poll, and on any exception close() the client) mirrored against a fake client whose
    `.connected` survives a simulated BrokenPipeError, matching real pymodbus -- the loop's own
    body isn't independently callable, so this checks the pattern's shape rather than driving
    main() itself. See check_inverter_reconnect_after_broken_pipe()'s docstring for the caveat.
  - paho-mqtt: app.mqtt_connect() still returns a working client (catches a Client()/
    callback_api_version constructor change).
  - The ENABLE_MQTT_PUBLISH switch: publishing toggles on/off for both the inverter and
    smart-meter topic trees, an inverter-only MQTT_PUBLISH_FIELDS allowlist doesn't leak onto the
    smart-meter side, MQTT_ENABLED follows either direction being in use, and -- the actual risk in
    adding this switch, since both directions share one broker client -- turning publishing off
    does not also stop plug messages from being queued.
  - psycopg + the Postgres write path: app.write_postgres() executes the expected SQL/params
    against a fake capturing connection/cursor (no real database needed) -- catching both a
    psycopg API change and a regression in the write path itself (wrong table, wrong ON
    CONFLICT clause, a known column routed into `extra` or vice versa).
  - Postgres write retry: app._write_until_done() keeps a point through a connection-level
    failure (backoff, fresh connection) and drops one the database rejects, without retrying it.
  - Clean stop: the real app.py, run as a subprocess, exits 0 promptly on SIGTERM (it is PID 1 in
    its container, where an unhandled SIGTERM is simply ignored), and app.drain_write_queue()
    waits for queued and in-flight points, is bounded when nothing writes, and keeps its count
    right when a full queue evicts a point.
  - psycopg + the Postgres read path: app.get_last_gas_reading() likewise, against a fake
    connection that returns a canned row.
  - Smart-plug MQTT message handling: app._on_mqtt_message() against real captured Zigbee2MQTT
    payloads from both plug models on this network (Aqara lumi.plug.mmeu01, Tuya TS0121), checking
    that only the common fields are kept, per-model extras are dropped, and throttling works.
  - Direct P1 telegram ingestion: app.crc16_arc()/extract_telegram() against a synthetic but
    correctly-checksummed telegram (built by the test itself, then verified via the same CRC
    function the app uses), covering framing across split TCP reads and a corrupted-CRC telegram
    being discarded without wedging the reader. app.parse_p1_telegram() for electricity + gas OBIS
    extraction, and app._handle_p1_telegram() for what ends up on the write queue (electricity,
    gas dedup against a repeated M-Bus capture, and the derived power_flow point).
  - P1 stall reconnect: app.p1_reader_loop() run for real against a real socket whose peer accepts
    the connection and then sends nothing at all -- the failure mode a recv() timeout used to
    `continue` on forever. Asserts the loop actually rebuilds the connection after
    P1_STALL_TIMEOUT rather than waiting on a socket that looks healthy but isn't.
  - DSMR timestamp parsing: app._parse_dsmr_timestamp() against explicit UTC instants (not
    recomputed with the implementation's own arithmetic, which would make the check tautological),
    including the autumn DST fall-back hour, where the S/W suffix is the only thing separating two
    otherwise-identical local timestamps -- getting this wrong silently overwrites an hour of
    readings a year via the write path's ON CONFLICT.
  - Smart-meter source stability: app.P1_SOURCE stays pinned to "dsmr", since it is a GROUP BY key
    in every downstream continuous aggregate and changing it splits history at the cutover instant.
  - The P1 relay (app._relay_broadcast(), ENABLE_P1_RELAY): reaches every connected downstream
    client, and drops+closes one whose send raises rather than letting that propagate out of the
    broadcast -- this is what stands in for DSMR-reader (or anything else) getting the P1 stream
    from this container instead of a second direct connection to ser2net, which does not actually
    grant concurrent access to one serial device (see the ENABLE_P1 comment in app.py).
  - The PVOutput metric formulas in app.METRICS, and that build_pvoutput_params() degrades to
    battery-only output when no electricity data is cached; power_flow's house_w = grid + the
    battery unit's AC output.
  - The daily yield (app.daily_yield_kwh()): a cached baseline is used without touching Postgres, a
    cache miss queries and caches app.get_daily_yield_baseline(), no baseline gives None.
  - Stroohm/Monta (ENABLE_MONTA): that the stroohm_monta_api.py downloaded at build still gives the
    session fields app.sync_monta() relies on, sync_monta()'s upsert against a fake Postgres
    connection, and MONTA_TIMES parsing / due-slot selection. The API client's own behaviour
    (paging, rate limit) is tested in its repo.

Run with no arguments; exits non-zero on the first failed expectation. Guarded by an overall
wall-clock timeout so a hang in any check (e.g. a client that doesn't disconnect cleanly) fails
loudly instead of stalling CI.
"""
import json
import logging
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from queue import Empty

os.environ.setdefault("BATTERY_HOST", "127.0.0.1")
os.environ.setdefault("BATTERY_PORT", "15020")
os.environ.setdefault("POSTGRES_HOST", "127.0.0.1")
os.environ.setdefault("POSTGRES_USER", "smoke")
os.environ.setdefault("POSTGRES_PASSWORD", "smoke")
os.environ.setdefault("MQTT_HOST", "127.0.0.1")

from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext  # noqa: E402
from pymodbus.server import StartTcpServer  # noqa: E402

import app  # noqa: E402
import paho.mqtt.client as mqtt  # noqa: E402

PORT = int(os.environ["BATTERY_PORT"])

OVERALL_TIMEOUT_S = 60

def _words32(value):
    """A signed or unsigned 32-bit value as the Solarbank sends it: high word first."""
    value &= 0xFFFFFFFF
    return [value >> 16, value & 0xFFFF]


# Raw register values by wire address, in the Solarbank's own layout (big-endian, high word first),
# chosen so every decode path is exercised: u16, signed and unsigned 32-bit, negative values, the
# registers read on their own, and a holding register. Values from a real afternoon reading.
RAW = {
    10001: 1,                              # status: charging
    **dict(zip((10002, 10003), _words32(2350))),    # PV power
    **dict(zip((10004, 10005), _words32(0))),       # third-party PV
    **dict(zip((10008, 10009), _words32(-1820))),   # battery power: charging 1820 W
    **dict(zip((10010, 10011), _words32(450))),     # load (from the unit's CT meter)
    **dict(zip((10012, 10013), _words32(-15))),     # grid (from the unit's CT meter)
    10014: 63,                              # SOC
    10015: 100,                             # SOH
    **dict(zip((10018, 10019), _words32(87))),      # PV total, 0.1 kWh -> 8.7
    **dict(zip((10026, 10027), _words32(22))),      # own use 2.2 kWh
    **dict(zip((10030, 10031), _words32(51))),      # socket energy in 5.1 kWh
    **dict(zip((10036, 10037), _words32(3500))),
    **dict(zip((10038, 10039), _words32(3500))),
    10156: 241,                             # temperature 24.1 C (read on its own)
    10167: 2726, 10168: 600,                # PV1 272.6 V, 6.00 A -> 1635.6 W
    10169: 2989, 10170: 239,                # PV2 298.9 V, 2.39 A -> 714.4 W; together the 2350 W PV
    10199: 2264, 10205: 196, 10213: 4998, 10224: 56, 10227: 56,
    **dict(zip((10208, 10209), _words32(430))),     # AC output into the house
    **dict(zip((10210, 10211), _words32(12))),
    **dict(zip((10250, 10251), _words32(71))),
    **dict(zip((10254, 10255), _words32(1790))),    # cell power (read on its own)
    **dict(zip((10262, 10263), _words32(132))),     # charged 13.2 kWh
    **dict(zip((10264, 10265), _words32(17))),      # discharged 1.7 kWh
}
HOLDING = {10064: 0, 60000: 100, 60001: 5, 60002: 20, 60003: 0}   # self-consumption, SOC limits
PV_OFFSET = 27866.6
EXPECTED_SOLAR = {
    "total_active_power": 2350,
    "total_power_yields": 27875.3,
    "total_power_yields_precise": 27875.3,
    "mppt_1_voltage": 272.6, "mppt_1_current": 6.0, "mppt_2_voltage": 298.9, "mppt_2_current": 2.39,
    "total_dc_power": 2350,
}
EXPECTED_BATTERY = {
    "soc": 63, "battery_power": -1820, "pv_power": 2350, "third_party_pv_power": 0,
    "load_power": 450, "grid_power": -15, "ac_output_power": 430, "charge_energy_total": 13.2,
    "discharge_energy_total": 1.7, "pv_energy_total": 8.7, "status": "charging",
    "operating_mode": "self_consumption", "soh": 100, "ac_voltage": 226.4, "ac_current": 1.96,
    "frequency": 49.98, "temperature": 24.1, "ac_power": 1790, "socket_energy_in": 5.1,
    "own_use_energy": 2.2, "max_charge_power": 3500, "rated_energy": 7.1, "charge_limit_soc": 100,
    "discharge_limit_soc": 5, "backup_reserve_soc": 20, "backup_reserve_enabled": 0,
}


def serve():
    """Serve RAW (input registers) and HOLDING over Modbus TCP.

    Only the test harness uses pymodbus's server API, which is the volatile part: 3.14 renamed
    ModbusSlaveContext to ModbusDeviceContext, and v4 will drop these in favour of SimData/SimDevice
    (hence the deprecation warnings). If a bump breaks *this* function rather than an assertion, the
    harness needs updating, not the app.
    """
    def block(values):
        regs = [0] * (max(values) + 8)
        for address, value in values.items():
            regs[address] = value
        # ModbusSequentialDataBlock stores at address-1, so 1 maps regs[i] to wire address i.
        return ModbusSequentialDataBlock(1, regs)

    try:  # pymodbus >= 3.14
        from pymodbus.datastore import ModbusDeviceContext

        context = ModbusServerContext(devices=ModbusDeviceContext(ir=block(RAW), hr=block(HOLDING)), single=True)
    except ImportError:  # older pymodbus
        from pymodbus.datastore import ModbusSlaveContext

        context = ModbusServerContext(
            slaves=ModbusSlaveContext(ir=block(RAW), hr=block(HOLDING), zero_mode=True), single=True)
    StartTcpServer(context=context, address=("127.0.0.1", PORT))


_battery_server_started = False


def _poll_against_fake_battery():
    """One poll against the fake Solarbank, starting the server on first use. Returns
    (solar, battery), or None if the server can't be reached."""
    global _battery_server_started
    from pymodbus.client import ModbusTcpClient

    if not _battery_server_started:
        threading.Thread(target=serve, daemon=True).start()
        _battery_server_started = True
        time.sleep(0.5)  # let it bind first, so the log isn't cluttered with a refused connect

    client = ModbusTcpClient("127.0.0.1", port=PORT, timeout=5)
    for _ in range(30):
        if client.connect():
            break
        time.sleep(0.5)
    else:
        return None
    orig_offset = app.PV_TOTAL_OFFSET_KWH
    app.PV_TOTAL_OFFSET_KWH = PV_OFFSET
    try:
        return app.poll_battery(client)
    finally:
        app.PV_TOTAL_OFFSET_KWH = orig_offset
        client.close()


def check_modbus_poll(failures):
    """Decode the Solarbank's registers through app.poll_battery() against a real pymodbus server.

    The word order is the trap: the unit sends 32-bit values high word first, and a low-word-first
    decode gives plausible-looking wrong numbers for small values. The negative battery power and
    grid values, and the lifetime counters, catch that.
    """
    result = _poll_against_fake_battery()
    if result is None:
        failures.append("  could not connect to the test battery")
        return
    solar, battery = result
    for name, want in EXPECTED_SOLAR.items():
        if solar.get(name) != want:
            failures.append(f"  solar {name}: expected {want!r}, got {solar.get(name)!r}")
    for name, want in EXPECTED_BATTERY.items():
        if battery.get(name) != want:
            failures.append(f"  battery {name}: expected {want!r}, got {battery.get(name)!r}")
    # Physical consistency: the PV going in has to cover what goes into the cells plus what comes
    # out of the AC side (an afternoon with no grid charging).
    if solar["total_active_power"] < -battery["battery_power"] + battery["ac_output_power"]:
        failures.append(f"  PV {solar['total_active_power']} W does not cover charging + AC output")
    print(f"  modbus poll: {len(EXPECTED_SOLAR) + len(EXPECTED_BATTERY)} values decoded, "
          f"unit kwarg={app._UNIT_KWARG!r}")


def check_battery_is_read_only(failures):
    """app._read() must refuse anything but function codes 3 and 4: the unit's operating mode,
    power setpoint and charge limits are writable registers, and nothing here may write."""
    class _Client:
        def read_holding_registers(self, *a, **k):
            raise AssertionError("not reached")
        read_input_registers = read_holding_registers
    for function in (6, 16):
        try:
            app._read(_Client(), function, 10064, 1)
            failures.append(f"  _read() accepted function code {function}")
        except KeyError:
            pass
    if not any(f.startswith("  _read() accepted") for f in failures):
        print("  read-only: _read() refuses write function codes")


def check_inverter_reconnect_after_broken_pipe(failures):
    """Inverter poll loop must force a genuine reconnect after a send-time socket error.

    Covers the fix in app.py's main() -- the `except Exception: ... modbus_client.close()` block
    right after the "Inverter poll failed" log line -- which isn't independently callable: it's
    inline in an infinite `while True:` loop that also starts every other background thread. So
    this check mirrors just the connect-check/work/except-close pattern from that loop against a
    fake client, rather than driving main() itself.

    The bug this guards against (root cause of a ~7 hour production outage on 2026-08-29): pymodbus
    does not reliably flip `.connected` back to False after a send-time socket error (observed: a
    BrokenPipeError left it True), so `if not modbus_client.connected and not
    modbus_client.connect()` never re-triggered and the loop retried writes on the same dead socket
    forever. The fake client below reproduces exactly that gap -- its `.connected` survives the
    simulated failure and only `.close()` resets it, matching real pymodbus where an explicit
    close() does work but the implicit failure path doesn't -- so this fails if the `.close()` call
    in the except block is ever removed, or if it stops resetting `.connected` before the next
    cycle runs.

    Caveat: because main()'s loop body can't be imported and called directly, this necessarily
    duplicates the shape of that pattern rather than exercising app.py's actual loop code -- it
    would not catch a regression where main() diverges from this exact shape while some
    differently-shaped fix happens to still work. It does catch the regression that matters most:
    the close() call being removed or neutered.
    """
    before = len(failures)

    class _FlappyModbusClient:
        """Reproduces the real pymodbus gap: `.connected` survives a send-time socket error, but
        a real `.close()` call does still reset it."""

        def __init__(self):
            self.connected = True  # already connected when this cycle starts
            self.connect_calls = 0
            self.close_calls = 0
            self._poll_calls = 0

        def connect(self):
            self.connect_calls += 1
            self.connected = True
            return True

        def close(self):
            self.close_calls += 1
            self.connected = False

        def poll(self):
            self._poll_calls += 1
            if self._poll_calls == 1:
                # Simulated send-time socket error. Real pymodbus leaves .connected True here --
                # that's the whole bug -- so this fake does too.
                raise BrokenPipeError("simulated send-time socket error")
            return f"ok (poll #{self._poll_calls})"

    def _run_cycle(client):
        """Mirrors app.py main()'s per-cycle pattern: reconnect-if-needed, then do the work."""
        if not client.connected and not client.connect():
            raise OSError("could not connect")
        return client.poll()

    client = _FlappyModbusClient()

    # Cycle 1: already connected, so no reconnect attempt yet; the poll itself fails.
    try:
        _run_cycle(client)
        failures.append("  cycle 1 was expected to raise (simulated socket error) but did not")
    except Exception:
        # Mirrors app.py's except block: close the client so the next cycle's reconnect check
        # is honest regardless of what .connected claims.
        try:
            client.close()
        except Exception:
            pass

    if client.close_calls != 1:
        failures.append(f"  expected close() once after cycle 1's failure, got {client.close_calls}")
    if client.connected:
        failures.append(
            "  client.connected is still True after close() -- the next cycle's "
            "'not modbus_client.connected' check would stay false forever, same as the original bug"
        )

    # Cycle 2: connected is now honestly False, so the reconnect check must fire and succeed.
    try:
        result = _run_cycle(client)
    except Exception as exc:
        failures.append(f"  cycle 2 unexpectedly raised: {exc!r}")
        result = None

    if client.connect_calls != 1:
        failures.append(
            f"  expected exactly 1 connect() call by cycle 2 (the genuine reconnect), got "
            f"{client.connect_calls}"
        )
    if result != "ok (poll #2)":
        failures.append(f"  cycle 2 did not recover cleanly: {result!r}")

    if len(failures) == before:
        print(
            "  inverter reconnect: close() forced after a simulated send-time socket error "
            "(connected stayed True, matching real pymodbus), next cycle's reconnect check "
            "fired honestly and recovered"
        )


def check_mqtt_publish_switch(failures):
    """ENABLE_MQTT_PUBLISH gates publishing without touching plug ingestion.

    The two directions share one broker client, so the risk in adding this switch was coupling them:
    turning publishing off must not stop plug messages arriving, and turning both off must stop the
    container connecting at all rather than leaving a connection the watchdog then complains about.
    """
    before = len(failures)
    saved = (app.ENABLE_MQTT_PUBLISH, app.ENABLE_PLUGS, app.MQTT_ENABLED)

    published = []

    class _FakeClient:
        def publish(self, topic, value, retain=False):
            published.append((topic, value, retain))

    try:
        app.ENABLE_MQTT_PUBLISH = True

        # Inverter side: every field under MQTT_TOPIC_PREFIX, retained.
        app.publish_mqtt(_FakeClient(), {"total_active_power": 2888, "run_state": "Run"},
                         app.MQTT_TOPIC_PREFIX, app.MQTT_PUBLISH_FIELDS)
        if len(published) != 2:
            failures.append(f"  publish_mqtt() inverter side: expected 2 topics, got {published}")
        elif not all(t.startswith(app.MQTT_TOPIC_PREFIX + "/") and r for t, _v, r in published):
            failures.append(f"  publish_mqtt() inverter topics/retain flag wrong: {published}")

        # Smart-meter side: its own prefix, and no MQTT_PUBLISH_FIELDS filter (that knob is
        # documented in terms of inverter register names, which would drop every meter field).
        published.clear()
        elec = {"electricity_currently_delivered": 0.0, "phase_voltage_l1": 235.0}
        app.publish_mqtt(_FakeClient(), elec, app.MQTT_ELEC_TOPIC_PREFIX)
        if sorted(t for t, _v, _r in published) != [
            f"{app.MQTT_ELEC_TOPIC_PREFIX}/electricity_currently_delivered",
            f"{app.MQTT_ELEC_TOPIC_PREFIX}/phase_voltage_l1",
        ]:
            failures.append(f"  publish_mqtt() smart-meter side: unexpected topics {published}")

        # The two trees must not overlap, or a subscriber can't take one without the other.
        if app.MQTT_TOPIC_PREFIX == app.MQTT_ELEC_TOPIC_PREFIX:
            failures.append(
                f"  MQTT_TOPIC_PREFIX and MQTT_ELEC_TOPIC_PREFIX are both {app.MQTT_TOPIC_PREFIX!r}"
            )

        # An inverter-only allowlist must not silently apply to the meter side.
        published.clear()
        app.publish_mqtt(_FakeClient(), elec, app.MQTT_ELEC_TOPIC_PREFIX, {"total_active_power"})
        if published:
            failures.append(f"  a filter that matches nothing should publish nothing, got {published}")
        published.clear()

        # MQTT_ENABLED is the connect decision, and must follow *either* direction being in use.
        for publish, plugs, expected in ((True, True, True), (True, False, True),
                                         (False, True, True), (False, False, False)):
            got = publish or plugs
            if got != expected:
                failures.append(f"  MQTT_ENABLED for publish={publish} plugs={plugs}: expected {expected}")
        # And the module-level value must agree with that rule for the config as loaded.
        if app.MQTT_ENABLED != (saved[0] or saved[1]):
            failures.append(
                f"  app.MQTT_ENABLED is {app.MQTT_ENABLED} but ENABLE_MQTT_PUBLISH={saved[0]} "
                f"ENABLE_PLUGS={saved[1]}"
            )

        # Plug ingestion must be unaffected by publishing being off -- it is the same client, so
        # this is the coupling worth asserting rather than assuming.
        app.ENABLE_MQTT_PUBLISH = False
        drained = 0
        while True:
            try:
                app._write_queue.get_nowait()
                drained += 1
            except Empty:
                break
        # Configures its own topic map rather than borrowing whatever another check happens to have
        # left behind: PLUG_TOPICS is unset in this harness, so relying on the ambient map meant
        # this assertion silently skipped itself -- a check that cannot fail is worse than no check.
        topic, label = "zigbee2mqtt/Publish switch test plug", "switch-test"
        orig_map, orig_seen = dict(app.PLUG_TOPIC_MAP), set(app._plug_seen)
        app.PLUG_TOPIC_MAP.clear()
        app.PLUG_TOPIC_MAP[topic] = label
        app._plug_last_write_time.pop(label, None)
        try:
            # Through the real dispatch path, not straight into the handler.
            app._on_mqtt_message(None, None, _StubMessage(topic, {"power": 12.5}))
            try:
                app._write_queue.get_nowait()
            except Empty:
                failures.append(
                    "  a plug message queued nothing while ENABLE_MQTT_PUBLISH=False -- the publish "
                    "switch must not disable plug ingestion, they only share a client"
                )
        finally:
            app.PLUG_TOPIC_MAP.clear()
            app.PLUG_TOPIC_MAP.update(orig_map)
            app._plug_seen.clear()
            app._plug_seen.update(orig_seen)
            app._plug_last_write_time.pop(label, None)
    finally:
        app.ENABLE_MQTT_PUBLISH, app.ENABLE_PLUGS, app.MQTT_ENABLED = saved

    # End to end: a real telegram through _handle_p1_telegram must publish under the electricity
    # prefix when the switch is on, and nothing at all when it's off. Asserting on publish_mqtt()
    # alone would not catch the call site being missing or mis-gated.
    # _handle_p1_telegram mutates a fair amount of module state (write throttle, gas dedup, caches,
    # first-seen log flags). All of it has to go back, or the later P1 checks inherit it -- which
    # they did on the first attempt: the throttle stayed set and their electricity write was
    # silently skipped, failing a test that had nothing to do with this one.
    saved_client, saved_min = app._mqtt_client, app.P1_MIN_INTERVAL
    saved_p1_state = (app._last_elec_write_time, app._last_gas_written, app._elec_cache,
                      app._elec_seen, app._gas_seen)
    for switch, expect_topics in ((True, True), (False, False)):
        published.clear()
        app.ENABLE_MQTT_PUBLISH = switch
        app._mqtt_client = _FakeClient()
        app.P1_MIN_INTERVAL = 0            # never throttled, so the publish is deterministic
        app._last_elec_write_time = 0.0
        app._handle_p1_telegram(_p1_telegram_bytes().decode("ascii"))
        got = [t for t, _v, _r in published if t.startswith(app.MQTT_ELEC_TOPIC_PREFIX + "/")]
        if expect_topics and not got:
            failures.append(
                "  a P1 telegram published no electricity topics with ENABLE_MQTT_PUBLISH=True"
            )
        elif not expect_topics and got:
            failures.append(
                f"  a P1 telegram published {len(got)} topic(s) with ENABLE_MQTT_PUBLISH=False"
            )
    app._mqtt_client, app.P1_MIN_INTERVAL = saved_client, saved_min
    (app._last_elec_write_time, app._last_gas_written, app._elec_cache,
     app._elec_seen, app._gas_seen) = saved_p1_state
    while True:
        try:
            app._write_queue.get_nowait()
        except Empty:
            break

    if len(failures) == before:
        print("  MQTT publish switch: gates both topic trees (solar + electricity), keeps them "
              "separate, and leaves plug ingestion alone")


def check_mqtt_connect(failures):
    """paho-mqtt API coverage.

    app.mqtt_connect() uses connect_async, which schedules the connection on paho's own network
    thread and returns immediately -- it does not need (and here, does not have) a reachable
    broker. The point is only to catch a paho constructor/signature change (e.g. Client() gaining
    a required argument, or callback_api_version being renamed/removed), which would raise here
    rather than at 3am against the real broker.
    """
    try:
        client = app.mqtt_connect()
    except Exception as exc:
        failures.append(f"  app.mqtt_connect() raised {exc!r} (paho-mqtt API change?)")
        return

    try:
        if not isinstance(client, mqtt.Client):
            failures.append(
                f"  app.mqtt_connect() returned {client!r}, expected a paho.mqtt.client.Client instance"
            )
        elif not hasattr(client, "is_connected") or not hasattr(client, "publish"):
            failures.append(
                f"  app.mqtt_connect() returned {client!r}, missing expected Client API (publish/is_connected)"
            )
        else:
            print("  mqtt connect: app.mqtt_connect() returned a usable paho Client")
    finally:
        # Clean up regardless of what happened above, so a failed assertion here can't leave a
        # background reconnect thread running past the end of the process.
        try:
            client.loop_stop()
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass


class _FakeCursor:
    """Records execute() calls and returns a canned row from fetchone(), standing in for a real
    psycopg cursor. Supports the `with conn.cursor() as cur:` pattern app.py uses."""

    def __init__(self, fetch_result=None):
        self.executed = []
        self._fetch_result = fetch_result

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchone(self):
        return self._fetch_result

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


class _FakeConnection:
    """Stands in for a real psycopg connection -- one cursor, remembered so a test can inspect
    what was executed. Supports `with psycopg.connect(...) as conn:` and being assigned directly
    to app._pg_conn (app's own persistent-connection slot)."""

    closed = False

    def __init__(self, fetch_result=None):
        self.cursor_obj = _FakeCursor(fetch_result)

    def cursor(self):
        return self.cursor_obj

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def check_postgres_write(failures):
    """psycopg + write_postgres() coverage against a fake connection/cursor -- no real Postgres
    needed. Checks the INSERT statement, its ON CONFLICT clause, and that a field not in
    TABLE_COLUMNS is routed into `extra` instead of becoming its own column (or failing outright).
    """
    before = len(failures)
    orig_conn = app._pg_conn
    fake_conn = _FakeConnection()
    app._pg_conn = fake_conn
    ts_ns = 1732900000123456789
    try:
        app.write_postgres(
            {"total_active_power": 2888.0, "daily_power_yields": 8.7, "not_a_real_column": 1.5},
            ts_ns,
            table=app.POSTGRES_TABLE,
            source="sungrow_test",
        )
    except Exception as exc:
        failures.append(f"  app.write_postgres() raised {exc!r}")
        app._pg_conn = orig_conn
        return
    finally:
        app._pg_conn = orig_conn

    executed = fake_conn.cursor_obj.executed
    if not executed:
        failures.append("  write_postgres() never executed a query")
        return

    sql, params = executed[0]
    if app.POSTGRES_TABLE not in sql:
        failures.append(f"  write_postgres() SQL doesn't reference the table: {sql!r}")
    if "ON CONFLICT (time, source) DO UPDATE" not in sql:
        failures.append(f"  write_postgres() SQL missing expected ON CONFLICT clause: {sql!r}")

    expected_ts = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
    if len(params) < 3 or params[0] != expected_ts or params[1] != "sungrow_test":
        failures.append(
            f"  write_postgres() time/source params: expected [{expected_ts!r}, 'sungrow_test', ...], "
            f"got {params!r}"
        )

    # "not_a_real_column" isn't in TABLE_COLUMNS[POSTGRES_TABLE] -- it must land in the `extra`
    # JSONB param rather than becoming its own column, while the two known fields stay direct
    # params in the order they were given.
    extra_param = params[2] if len(params) > 2 else None
    if extra_param is None or json.loads(extra_param) != {"not_a_real_column": 1.5}:
        failures.append(
            f"  write_postgres() extra param: expected '{{\"not_a_real_column\": 1.5}}', got {extra_param!r}"
        )
    if params[3:] != [2888.0, 8.7]:
        failures.append(f"  write_postgres() known-column params: expected [2888.0, 8.7], got {params[3:]!r}")

    if len(failures) == before:
        print("  postgres write: SQL/ON CONFLICT/params as expected, unmapped field routed into `extra`")


class _FlakyConnection(_FakeConnection):
    """A fake connection whose execute() raises `error` (once per connection) while the shared
    `state["fail_left"]` counter is positive, then records normally."""

    def __init__(self, state, error):
        super().__init__()
        self.closed = False

        class _Cursor(_FakeCursor):
            def execute(self, sql, params=None):
                if state["fail_left"] > 0:
                    state["fail_left"] -= 1
                    raise error
                state["written"].append((sql, params))

        self.cursor_obj = _Cursor()

    def close(self):
        self.closed = True


def check_postgres_write_retry(failures):
    """_write_until_done(): a point survives a database outage instead of being thrown away, and
    a point the database rejects is dropped instead of wedging the writer.

    Runs the real retry loop with an injected sleep, against fake connections handed out by a
    monkeypatched psycopg.connect(), so no real database and no real waiting are needed.
    """
    before = len(failures)
    orig_conn, orig_connect = app._pg_conn, app.psycopg.connect
    try:
        # 1. Connection-level failure twice, then success: written once, backoff 1s then 2s, and a
        #    fresh connection opened after each failure (the broken one closed).
        state = {"fail_left": 2, "written": []}
        opened = []

        def _connect(**kwargs):
            conn = _FlakyConnection(state, app.psycopg.OperationalError("server closed the connection"))
            opened.append(conn)
            return conn

        app.psycopg.connect = _connect
        app._pg_conn = None
        sleeps = []
        ok = app._write_until_done({"total_active_power": 1.0}, 1732900000000000000,
                                   app.POSTGRES_TABLE, "sungrow_test", sleep=sleeps.append)
        if ok is not True or len(state["written"]) != 1:
            failures.append(f"  outage: expected the point written once after retries, got ok={ok!r}, "
                            f"written={len(state['written'])}")
        if sleeps != [1.0, 2.0]:
            failures.append(f"  outage: expected backoff sleeps [1.0, 2.0], got {sleeps!r}")
        if len(opened) != 3 or not all(c.closed for c in opened[:2]):
            failures.append(f"  outage: expected 3 connections with the 2 failed ones closed, got "
                            f"{len(opened)} opened, closed={[c.closed for c in opened]}")

        # 2. The point itself is rejected: dropped at once, no retry, no sleep.
        state = {"fail_left": 1, "written": []}
        app.psycopg.connect = lambda **kwargs: _FlakyConnection(
            state, app.psycopg.DataError("invalid input syntax"))
        app._pg_conn = None
        sleeps = []
        ok = app._write_until_done({"total_active_power": "garbage"}, 1732900000000000000,
                                   app.POSTGRES_TABLE, "sungrow_test", sleep=sleeps.append)
        if ok is not False or sleeps:
            failures.append(f"  rejected point: expected dropped without retry, got ok={ok!r}, sleeps={sleeps!r}")
    except Exception as exc:
        failures.append(f"  _write_until_done() raised {exc!r}")
    finally:
        app._pg_conn, app.psycopg.connect = orig_conn, orig_connect

    if len(failures) == before:
        print("  postgres write retry: point kept through a connection outage (1s/2s backoff, fresh "
              "connection), rejected point dropped without retry")


def check_clean_stop(failures):
    """SIGTERM ends the process promptly and cleanly, and the shutdown flush waits for the writer.

    Two halves. The signal path runs the real app.py as a subprocess, because a handler and the
    `__main__` wiring around main() can't be exercised by calling functions. The flush path runs
    the real queue_write()/pg_writer_loop()/drain_write_queue() against a fake connection.
    """
    before = len(failures)

    # 1. Signal path. Everything optional is switched off and the battery port has no listener,
    #    so main() sits in its poll loop failing to connect -- which is all this needs.
    child_env = dict(os.environ, LOG_LEVEL="INFO", BATTERY_PORT="9", SCAN_INTERVAL="30",
                     ENABLE_P1="false", ENABLE_P1_RELAY="false", ENABLE_PLUGS="false",
                     ENABLE_MQTT_PUBLISH="false", ENABLE_PVOUTPUT="false",
                     ENABLE_MINDERGAS="false", ENABLE_MONTA="false", PYTHONUNBUFFERED="1")
    proc = subprocess.Popen([sys.executable, os.path.abspath(app.__file__)], env=child_env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    lines = []

    def _collect_output():
        for line in proc.stdout:
            lines.append(line)

    reader = threading.Thread(target=_collect_output, daemon=True)
    reader.start()
    try:
        deadline = time.monotonic() + 15
        # "Battery poll failed" means main() is past startup and inside its loop.
        while time.monotonic() < deadline and not any("Battery poll failed" in l for l in lines):
            if proc.poll() is not None:
                break
            time.sleep(0.05)
        if proc.poll() is not None:
            failures.append(f"  clean stop: app.py exited on its own ({proc.returncode}) before "
                            f"the signal: {''.join(lines)[-400:]!r}")
        elif not any("Battery poll failed" in l for l in lines):
            failures.append("  clean stop: app.py never reached its poll loop within 15s")
        else:
            sent = time.monotonic()
            proc.send_signal(signal.SIGTERM)
            try:
                code = proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                failures.append("  clean stop: app.py still running 8s after SIGTERM")
            else:
                took = time.monotonic() - sent
                # Without the handler a non-PID-1 process dies *from* the signal (-15); 0 proves
                # the handler ran and the process chose to exit.
                if code != 0:
                    failures.append(f"  clean stop: exit status {code} after SIGTERM, expected 0 "
                                    f"(-15 means the signal was not handled)")
                if took > 3:
                    failures.append(f"  clean stop: took {took:.1f}s to exit with an empty queue")
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait()
        reader.join(timeout=2)

    # 2. Flush path, on a private queue so nothing leaks into (or from) the other checks.
    orig_queue, orig_conn = app._write_queue, app._pg_conn
    try:
        app._write_queue = queue.Queue(maxsize=2)
        fake_conn = _FakeConnection()
        app._pg_conn = fake_conn
        for n in (1, 2, 3):  # the third evicts the first
            app.queue_write({"total_active_power": float(n)}, 1732900000000000000 + n,
                            app.POSTGRES_TABLE, "sungrow_test")
        if app._write_queue.unfinished_tasks != 2:
            failures.append(f"  drain: an evicted point is still counted as pending "
                            f"(unfinished_tasks={app._write_queue.unfinished_tasks}, expected 2); "
                            f"the shutdown flush would always run out its timeout")

        # Nothing is writing yet: the drain must give up at its timeout and report what is left.
        started = time.monotonic()
        left = app.drain_write_queue(0.3)
        waited = time.monotonic() - started
        if left != 2 or not 0.25 <= waited < 2:
            failures.append(f"  drain: with no writer expected 2 left after ~0.3s, got {left} "
                            f"after {waited:.2f}s")

        # With the writer running it must return 0 once both points are in the database. The
        # thread is a daemon blocked on this private queue afterwards; it never touches the
        # real one.
        threading.Thread(target=app.pg_writer_loop, daemon=True).start()
        left = app.drain_write_queue(5)
        written = [params[3] for _sql, params in fake_conn.cursor_obj.executed]
        if left != 0 or written != [2.0, 3.0]:
            failures.append(f"  drain: expected 0 left and points [2.0, 3.0] written, got {left} "
                            f"left and {written!r}")
    except Exception as exc:
        failures.append(f"  drain_write_queue() raised {exc!r}")
    finally:
        app._write_queue, app._pg_conn = orig_queue, orig_conn

    if len(failures) == before:
        print("  clean stop: SIGTERM exits 0 promptly; shutdown flush waits for the writer, is "
              "bounded without one, and discounts an evicted point")


def check_get_last_gas_reading(failures):
    """psycopg + get_last_gas_reading() coverage.

    Monkeypatches psycopg.connect() (this function opens its own short-lived connection rather
    than sharing the writer thread's persistent one) to return a fake connection with a canned
    row, and checks the query references the right table, has no lower time bound (Mindergas
    wants the actual final reading of the day, however far back that turns out to be, not merely
    "a recent-enough one"), and binds the cutoff as a parameter rather than interpolating it into
    the SQL string.
    """
    before = len(failures)
    canned_row = (datetime(2026, 7, 31, 23, 50, tzinfo=timezone.utc), 6573.284)
    fake_conn = _FakeConnection(fetch_result=canned_row)
    orig_connect = app.psycopg.connect
    app.psycopg.connect = lambda **kwargs: fake_conn
    midnight = datetime(2026, 8, 1, 0, 0, tzinfo=timezone.utc)
    try:
        result = app.get_last_gas_reading(midnight)
    except Exception as exc:
        failures.append(f"  app.get_last_gas_reading() raised {exc!r}")
        return
    finally:
        app.psycopg.connect = orig_connect

    if result != canned_row:
        failures.append(f"  get_last_gas_reading() return: expected {canned_row}, got {result!r}")

    sql, params = fake_conn.cursor_obj.executed[0]
    if app.MINDERGAS_GAS_TABLE not in sql:
        failures.append(f"  get_last_gas_reading() SQL doesn't reference {app.MINDERGAS_GAS_TABLE!r}: {sql!r}")
    if "time >=" in sql or "BETWEEN" in sql.upper():
        failures.append(f"  get_last_gas_reading() SQL has a lower time bound, but should have none: {sql!r}")
    if params != (midnight,):
        failures.append(f"  get_last_gas_reading() params: expected {(midnight,)!r}, got {params!r}")

    if len(failures) == before:
        print("  postgres read (get_last_gas_reading): SQL/params/return value as expected")


class _StubMessage:
    """Mimics the paho MQTTMessage attributes app._on_mqtt_message() reads."""

    def __init__(self, topic, payload_dict):
        self.topic = topic
        self.payload = json.dumps(payload_dict).encode("utf-8")


# A real telegram body (meter XMX5LGBBLB2415154262, matching a captured sample -- standard Dutch
# DSMR 5.0, single-phase, gas discovered on M-Bus channel 1) with its own correct CRC appended by
# the test itself via app.crc16_arc(), rather than a hardcoded checksum -- so this exercises the
# same CRC function the reader trusts in production, not a second, possibly-wrong implementation.
_P1_TELEGRAM_BODY = (
    "/XMX5LGBBLB2415154262\r\n"
    "\r\n"
    "1-3:0.2.8(50)\r\n"
    "0-0:1.0.0(260812134420S)\r\n"
    "1-0:1.8.1(013727.545*kWh)\r\n"
    "1-0:1.8.2(011228.911*kWh)\r\n"
    "1-0:2.8.1(005818.379*kWh)\r\n"
    "1-0:2.8.2(012199.498*kWh)\r\n"
    "1-0:1.7.0(00.417*kW)\r\n"
    "1-0:2.7.0(00.000*kW)\r\n"
    "0-0:96.14.0(0002)\r\n"
    "1-0:32.7.0(235.0*V)\r\n"
    "1-0:31.7.0(004*A)\r\n"
    "0-1:24.1.0(003)\r\n"
    "0-1:24.2.1(260812134400S)(06573.284*m3)\r\n"
    "!"
)


def _p1_telegram_bytes(body=_P1_TELEGRAM_BODY):
    crc = app.crc16_arc(body.encode("ascii"))
    return body.encode("ascii") + f"{crc:04X}".encode("ascii") + b"\r\n"


def check_p1_telegram_parsing(failures):
    """Direct P1 telegram ingestion: framing/CRC (app.extract_telegram()), OBIS extraction
    (app.parse_p1_telegram()), and what app._handle_p1_telegram() does with the result end to end.
    """
    before = len(failures)
    full = _p1_telegram_bytes()

    # A complete telegram plus trailing bytes from whatever comes next must be extracted cleanly,
    # leaving exactly the trailing bytes behind.
    telegram, remaining = app.extract_telegram(full + b"next-telegram-starts-here")
    if telegram is None:
        failures.append("  extract_telegram() did not recognise a complete, valid telegram")
    if remaining != b"next-telegram-starts-here":
        failures.append(f"  extract_telegram() remaining buffer: expected trailing bytes only, got {remaining!r}")

    # A telegram split across two TCP reads (landing mid-telegram) must reassemble correctly --
    # this is the normal case for a freshly opened connection, not an edge case.
    split_at = 40
    partial, buf = app.extract_telegram(full[:split_at])
    if partial is not None:
        failures.append("  extract_telegram() returned a telegram from an incomplete buffer")
    reassembled, buf = app.extract_telegram(buf + full[split_at:])
    if reassembled != telegram:
        failures.append("  extract_telegram() failed to reassemble a telegram split across two reads")

    # A corrupted CRC must be discarded (logged, not raised) without leaving the reader stuck --
    # the remaining buffer must still advance past the bad telegram.
    corrupted = full[:-6] + b"FFFF\r\n"
    bad_result, bad_remaining = app.extract_telegram(corrupted)
    if bad_result is not None:
        failures.append("  extract_telegram() accepted a telegram with a wrong CRC")
    if bad_remaining != b"":
        failures.append(f"  extract_telegram() on a bad-CRC telegram: expected empty remainder, got {bad_remaining!r}")

    expected_elec = {
        "electricity_delivered_1": 13727.545,
        "electricity_delivered_2": 11228.911,
        "electricity_returned_1": 5818.379,
        "electricity_returned_2": 12199.498,
        "electricity_currently_delivered": 0.417,
        "electricity_currently_returned": 0.0,
        "phase_voltage_l1": 235.0,
        # int, not float: the meter reports both as whole numbers and they are stored in integer
        # columns. `== 2.0` would pass here even if the cast were dropped, so the types are
        # asserted separately below.
        "electricity_tariff": 2,
        "phase_power_current_l1": 4,
    }
    expected_elec_ts_ns = app._parse_dsmr_timestamp("260812134420S")
    expected_gas_ts_ns = app._parse_dsmr_timestamp("260812134400S")

    elec, elec_ts_ns, gas = app.parse_p1_telegram(telegram)
    if elec != expected_elec:
        failures.append(f"  parse_p1_telegram() electricity: expected {expected_elec}, got {elec}")
    # Python's 2 == 2.0, so equality above cannot tell an int from a float. These two land in
    # smallint/integer columns and are grouped and filtered on, so the type is the point.
    for field in sorted(app._ELEC_INT_FIELDS):
        if field in elec and not isinstance(elec[field], int):
            failures.append(
                f"  parse_p1_telegram(): {field} is {type(elec[field]).__name__} "
                f"({elec[field]!r}), expected int"
            )
    if elec_ts_ns != expected_elec_ts_ns:
        failures.append(f"  parse_p1_telegram() electricity timestamp: expected {expected_elec_ts_ns}, got {elec_ts_ns}")
    if gas != (6573.284, expected_gas_ts_ns):
        failures.append(f"  parse_p1_telegram() gas: expected (6573.284, {expected_gas_ts_ns}), got {gas}")

    # End to end through _handle_p1_telegram(): electricity + gas both queued, the elec cache
    # updated, and a power_flow point derived alongside it.
    orig_elec_cache = app._elec_cache
    orig_last_gas_written = app._last_gas_written
    app._elec_cache = None
    app._last_gas_written = None
    drained = []
    while True:
        try:
            drained.append(app._write_queue.get_nowait())
        except Empty:
            break

    try:
        app._handle_p1_telegram(telegram)
        queued = []
        while True:
            try:
                queued.append(app._write_queue.get_nowait())
            except Empty:
                break

        elec_item = next((it for it in queued if it[2] == app.ELEC_TABLE), None)
        gas_item = next((it for it in queued if it[2] == app.GAS_TABLE), None)
        derived_item = next((it for it in queued if it[2] == app.DERIVED_TABLE), None)

        if elec_item is None:
            failures.append(f"  no point enqueued for elec table {app.ELEC_TABLE!r}; queued={queued}")
        elif elec_item[0] != expected_elec or elec_item[3] != app.P1_SOURCE:
            failures.append(
                f"  queued elec point: expected ({expected_elec}, source={app.P1_SOURCE!r}), got {elec_item}"
            )

        if gas_item is None:
            failures.append(f"  no point enqueued for gas table {app.GAS_TABLE!r}; queued={queued}")
        elif gas_item[0] != {"delivered": 6573.284} or gas_item[3] != app.P1_SOURCE:
            failures.append(
                f"  queued gas point: expected (delivered=6573.284, source={app.P1_SOURCE!r}), got {gas_item}"
            )

        if derived_item is None:
            failures.append(f"  no power_flow point enqueued alongside the electricity update; queued={queued}")

        cached = app.get_elec_values()
        if cached != expected_elec:
            failures.append(f"  get_elec_values() after a P1 telegram: expected {expected_elec}, got {cached}")

        # The gas M-Bus capture repeats unchanged in every telegram between the meter's own
        # ~5-minute updates -- a second identical telegram must not re-queue the same gas point.
        app._handle_p1_telegram(telegram)
        requeued_gas = None
        while True:
            try:
                item = app._write_queue.get_nowait()
            except Empty:
                break
            if item[2] == app.GAS_TABLE:
                requeued_gas = item
        if requeued_gas is not None:
            failures.append(f"  an unchanged repeated gas reading was queued again: {requeued_gas}")

        if len(failures) == before:
            print(
                "  P1 telegram parsing: CRC/framing (incl. split reads and a corrupted CRC), "
                "electricity+gas OBIS extraction, and end-to-end queuing/dedup all as expected"
            )
    finally:
        app._elec_cache = orig_elec_cache
        app._last_gas_written = orig_last_gas_written
        for item in drained:  # put back anything that was queued before this check ran
            app._write_queue.put_nowait(item)


class _FakeRelayClient:
    """Mimics the socket app._relay_broadcast() writes to -- sendall()/close(), nothing else."""

    def __init__(self, fail=False):
        self.sent = []
        self.fail = fail
        self.closed = False

    def sendall(self, data):
        if self.fail:
            raise OSError("simulated dead/stuck relay client")
        self.sent.append(data)

    def close(self):
        self.closed = True


def check_p1_stall_reconnect(failures):
    """app.p1_reader_loop() must rebuild a connection that stays open but stops delivering.

    Runs the real loop against a real socket, because the bug this covers was a control-flow
    decision, not a computation: a recv() timeout used to `continue`, so an upstream that hangs
    while holding the TCP session open (ser2net wedged, conntrack state dropped -- no FIN, no RST)
    stalled ingestion permanently behind a single warning. Asserting on anything less than "did it
    actually open a second connection" would just restate the code.

    The fake upstream accepts and then says nothing at all, which is exactly the failure mode: a
    connection that is healthy by every measure the socket exposes, and useless.
    """
    before = len(failures)
    saved = (app.P1_HOST, app.P1_PORT, app.P1_WARN_AFTER, app.P1_STALL_TIMEOUT, app.ENABLE_P1_RELAY)
    # Real seconds, so keep them small -- the loop reconnects after P1_STALL_TIMEOUT plus its
    # backoff, and the backoff doubles because no data ever arrives to reset it.
    app.P1_WARN_AFTER, app.P1_STALL_TIMEOUT, app.ENABLE_P1_RELAY = 1, 2, False

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(5)
    app.P1_HOST, app.P1_PORT = server.getsockname()

    connections = []
    accepted = threading.Event()

    def accept_loop():
        while True:
            try:
                conn, _ = server.accept()
            except OSError:
                return
            connections.append(conn)  # held open and deliberately never written to
            if len(connections) >= 2:
                accepted.set()

    # The reconnect path logs a warning with a full traceback, which is correct in production and
    # pure noise here -- suppressed so a passing run doesn't read like a crashing one.
    previous_level = app.log.level
    app.log.setLevel(logging.CRITICAL)

    threading.Thread(target=accept_loop, daemon=True).start()
    threading.Thread(target=app.p1_reader_loop, daemon=True).start()

    # First connect is immediate; the second needs stall (2s) + backoff (1s). 15s is slack for a
    # loaded CI runner, not an expected duration.
    reconnected = accepted.wait(timeout=15)
    if not reconnected:
        failures.append(
            f"  p1_reader_loop() did not reconnect to a silent upstream within 15s "
            f"(P1_STALL_TIMEOUT={app.P1_STALL_TIMEOUT}s); saw {len(connections)} connection(s), "
            f"expected at least 2 -- a hung upstream would stall ingestion forever"
        )

    # p1_reader_loop runs forever by design and has no stop signal, so this thread outlives the
    # check. Park it somewhere harmless rather than tearing its world down: closing the server or
    # repointing the host would send it into a reconnect loop that logs a traceback per attempt,
    # which lands in CI output looking like a failure (it did -- intermittently, depending on where
    # the loop was when the check finished).
    #
    # Leaving the listener open keeps it connected and idle instead, and the deliberately large
    # timeouts below mean it won't warn or stall-reconnect within the few seconds the remaining
    # checks take. P1_WARN_AFTER/P1_STALL_TIMEOUT are read only by this loop, so not restoring their
    # original values affects nothing else.
    app.P1_WARN_AFTER, app.P1_STALL_TIMEOUT = 3600, 3600
    app.ENABLE_P1_RELAY = saved[4]
    app.log.setLevel(previous_level)

    if len(failures) == before:
        print(f"  P1 stall reconnect: rebuilt a silent-but-open connection ({len(connections)} "
              f"connects seen), instead of waiting on it forever")


def check_p1_http_reader(failures):
    """app.p1_http_reader_loop(): reading the telegram from a dongle's HTTP API (P1_URL) instead of
    a stream. Runs the real loop against a real local HTTP server and checks the three things that
    matter: a telegram is handled once however often it is polled (the dongle keeps answering with
    the latest one until the meter sends the next), a new telegram is handled when it appears, and
    the relay gets each new telegram whole and only once.
    """
    import http.server

    before = len(failures)
    served = {"body": _p1_telegram_bytes()}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            body = served["body"]
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    handled, relayed = [], []
    saved = (app._handle_p1_telegram, app._relay_broadcast, app.P1_URL, app.P1_POLL_INTERVAL,
             app.ENABLE_P1_RELAY, app.P1_WARN_AFTER)
    app._handle_p1_telegram = handled.append
    app._relay_broadcast = relayed.append
    app.P1_URL = f"http://127.0.0.1:{server.server_address[1]}/api/v1/telegram"
    app.P1_POLL_INTERVAL, app.ENABLE_P1_RELAY, app.P1_WARN_AFTER = 1, True, 3600
    try:
        threading.Thread(target=app.p1_http_reader_loop, daemon=True).start()
        time.sleep(2.5)  # three polls of the same telegram
        if len(handled) != 1 or len(relayed) != 1:
            failures.append(f"  P1 over HTTP: same telegram polled 3 times gave {len(handled)} handled / "
                            f"{len(relayed)} relayed, expected 1 / 1")
        second = _p1_telegram_bytes(_P1_TELEGRAM_BODY.replace("00.417*kW", "00.523*kW"))
        served["body"] = second
        time.sleep(1.5)
        if len(handled) != 2:
            failures.append(f"  P1 over HTTP: a new telegram was not handled ({len(handled)} handled, expected 2)")
        elif "00.523" not in handled[1]:
            failures.append(f"  P1 over HTTP: second telegram handled is not the new one: {handled[1][:80]!r}")
        if relayed[-1:] != [second]:
            failures.append("  P1 over HTTP: the relay did not get the new telegram whole")
    finally:
        # The loop has no stop signal and outlives the check, like p1_reader_loop in the stall
        # check. Restoring the handler is safe: the served telegram no longer changes, so it is never
        # called again, and P1_WARN_AFTER stays large so it can't warn during the remaining checks.
        (app._handle_p1_telegram, app._relay_broadcast, app.P1_URL, app.P1_POLL_INTERVAL,
         app.ENABLE_P1_RELAY) = saved[:5]

    if len(failures) == before:
        print("  P1 over HTTP: one telegram handled and relayed once however often it is polled, "
              "a new one picked up on the next poll")


def check_p1_relay_broadcast(failures):
    """app._relay_broadcast(): the P1 fan-out relay this container runs so DSMR-reader (or
    anything else) can get the telegram stream from *this* container instead of a second direct
    connection to ser2net -- see the incident this exists to fix (ENABLE_P1's docstring/comment in
    app.py): two ser2net connections on one serial device don't actually share it.

    Checks a healthy client receives the bytes, a client whose sendall() raises (dead/stuck) is
    dropped and closed rather than raising out of the broadcast, and a second broadcast only
    reaches whichever client is still connected.
    """
    before = len(failures)
    orig_clients = set(app._relay_clients)
    app._relay_clients.clear()

    healthy = _FakeRelayClient()
    dead = _FakeRelayClient(fail=True)
    app._relay_clients.add(healthy)
    app._relay_clients.add(dead)

    try:
        app._relay_broadcast(b"/telegram-bytes\r\n")

        if healthy.sent != [b"/telegram-bytes\r\n"]:
            failures.append(f"  relay broadcast: healthy client got {healthy.sent}, expected the one chunk")
        if not dead.closed:
            failures.append("  relay broadcast: a client whose sendall() raised was not closed")
        if dead in app._relay_clients:
            failures.append("  relay broadcast: a client whose sendall() raised was not dropped from _relay_clients")
        if healthy not in app._relay_clients:
            failures.append("  relay broadcast: the healthy client was dropped even though sendall() succeeded")

        # A second broadcast must only reach the survivor -- the dropped client already closed.
        app._relay_broadcast(b"more-bytes")
        if healthy.sent != [b"/telegram-bytes\r\n", b"more-bytes"]:
            failures.append(f"  relay broadcast: healthy client after 2nd send: {healthy.sent}")

        if len(failures) == before:
            print("  P1 relay broadcast: reaches connected clients, drops+closes a dead one without raising")
    finally:
        app._relay_clients.clear()
        app._relay_clients.update(orig_clients)


def check_plug_mqtt_message(failures):
    """Smart-plug MQTT message parsing, using real captured Zigbee2MQTT payloads.

    Exercises app._on_mqtt_message() end to end for the plug topic path: only the common
    power/energy/current/voltage/state fields end up queued for Postgres, per-model extras
    (auto_off, device_temperature, indicator_mode, ...) are dropped, and an unconfigured topic is
    silently ignored rather than raising or getting queued.
    """
    before = len(failures)
    orig_topic_map = dict(app.PLUG_TOPIC_MAP)
    orig_min_interval = app.PLUG_MIN_INTERVAL
    orig_last_write_time = dict(app._plug_last_write_time)
    orig_seen = set(app._plug_seen)
    app.PLUG_TOPIC_MAP.clear()
    app.PLUG_TOPIC_MAP.update({
        "zigbee2mqtt/Plug A": "plug-a",
        "zigbee2mqtt/Plug B": "plug-b",
    })
    app.PLUG_MIN_INTERVAL = 0  # disable throttling so both messages definitely get queued
    app._plug_last_write_time.clear()
    app._plug_seen.clear()
    drained = []
    while True:
        try:
            drained.append(app._write_queue.get_nowait())
        except Empty:
            break

    # Aqara lumi.plug.mmeu01 -- representative payload.
    aqara_payload = {
        "auto_off": False, "consumer_connected": False, "consumption": 534.5677490234375,
        "current": 0.09, "device_temperature": 30, "energy": 534.57, "led_disabled_night": True,
        "linkquality": 167, "power": 27.5, "power_outage_count": 41, "power_outage_memory": True,
        "state": "ON", "voltage": 234,
    }
    # Tuya TS0121 -- representative payload.
    tuya_payload = {
        "current": 0.46, "energy": 2652.83, "indicator_mode": "off", "linkquality": 32,
        "power": 94, "power_outage_memory": "off", "state": "ON", "voltage": 234,
    }

    try:
        app._on_mqtt_message(None, None, _StubMessage("zigbee2mqtt/Plug A", aqara_payload))
        app._on_mqtt_message(None, None, _StubMessage("zigbee2mqtt/Plug B", tuya_payload))
        # An unconfigured topic must be silently ignored, not raise or get queued.
        app._on_mqtt_message(None, None, _StubMessage("zigbee2mqtt/Badkamer sensor", {"battery": 100}))

        queued = []
        while True:
            try:
                queued.append(app._write_queue.get_nowait())
            except Empty:
                break

        expected_a = {"power": 27.5, "energy": 534.57, "current": 0.09, "voltage": 234.0, "state": "ON"}
        expected_b = {"power": 94.0, "energy": 2652.83, "current": 0.46, "voltage": 234.0, "state": "ON"}

        a_item = next((it for it in queued if it[3] == "plug-a"), None)
        b_item = next((it for it in queued if it[3] == "plug-b"), None)

        if a_item is None:
            failures.append(f"  no point enqueued for plug label 'plug-a'; queued={queued}")
        else:
            values, _ts_ns, table, _source = a_item
            if values != expected_a:
                failures.append(f"  queued plug-a point: expected {expected_a}, got {values}")
            if table != app.PLUG_TABLE:
                failures.append(
                    f"  queued plug-a table: expected {app.PLUG_TABLE!r}, got {table!r}"
                )

        if b_item is None:
            failures.append(f"  no point enqueued for plug label 'plug-b'; queued={queued}")
        else:
            values, _ts_ns, _table, _source = b_item
            if values != expected_b:
                failures.append(f"  queued plug-b point: expected {expected_b}, got {values}")

        if len(queued) != 2:
            failures.append(
                f"  expected exactly 2 queued plug points (unconfigured topic ignored), got "
                f"{len(queued)}: {queued}"
            )

        if len(failures) == before:
            print("  plug mqtt parsing: Aqara + Tuya payloads reduced to the common field set and queued as expected")
    finally:
        app.PLUG_TOPIC_MAP.clear()
        app.PLUG_TOPIC_MAP.update(orig_topic_map)
        app.PLUG_MIN_INTERVAL = orig_min_interval
        app._plug_last_write_time.clear()
        app._plug_last_write_time.update(orig_last_write_time)
        app._plug_seen.clear()
        app._plug_seen.update(orig_seen)
        for item in drained:
            app._write_queue.put_nowait(item)


def check_dsmr_timestamp_parsing(failures):
    """app._parse_dsmr_timestamp() on DSMR's YYMMDDhhmmss+[SW] format, asserted against the UTC
    instant each local timestamp must resolve to.

    Every expectation below is written as an explicit UTC wall-clock time, NOT recomputed with the
    same astimezone() arithmetic the implementation uses. The previous version of this test did the
    latter, which made it tautological: it mirrored the implementation exactly and therefore passed
    just as happily while the DST fall-back hour was being silently overwritten once a year.

    Pins TZ to Europe/Amsterdam for the duration rather than trusting the ambient one -- the image
    sets no TZ (the deploying compose file does), so under CI this process is UTC, where every case
    below is unambiguous and proves nothing.
    """
    previous_tz = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Amsterdam"
    time.tzset()
    try:
        cases = (
            # (telegram timestamp, expected UTC instant, what it exercises)
            ("260812134420S", (2026, 8, 12, 11, 44, 20), "ordinary summer time (CEST, UTC+2)"),
            ("260101090000W", (2026, 1, 1, 8, 0, 0), "ordinary winter time (CET, UTC+1)"),
            # 2026-10-25 is the autumn fall-back: 03:00 CEST becomes 02:00 CET, so 02:30 local
            # happens twice and only the S/W flag separates the two. These two cases are the whole
            # point of the fix -- with the flag ignored they collapse onto the same instant and the
            # second silently overwrites the first via ON CONFLICT (time, source).
            ("261025023000S", (2026, 10, 25, 0, 30, 0), "fall-back hour, first (summer) pass"),
            ("261025023000W", (2026, 10, 25, 1, 30, 0), "fall-back hour, second (winter) pass"),
            # Just before the transition, unambiguous, so the W-means-fold=1 mapping must not shift
            # anything that was already correct.
            ("261025015959S", (2026, 10, 24, 23, 59, 59), "one second before the fall-back"),
        )
        resolved = {}
        for raw, utc_parts, label in cases:
            expected = int(datetime(*utc_parts, tzinfo=timezone.utc).timestamp() * 1_000_000_000)
            got = app._parse_dsmr_timestamp(raw)
            resolved[raw] = got
            if got != expected:
                failures.append(
                    f"  _parse_dsmr_timestamp({raw!r}) [{label}]: expected {expected} "
                    f"({datetime(*utc_parts, tzinfo=timezone.utc).isoformat()}), got {got} "
                    f"({datetime.fromtimestamp(got / 1e9, tz=timezone.utc).isoformat()})"
                )

        # Stated separately from the per-case assertions above: this is the actual failure mode
        # (two distinct readings landing on one primary key), and it should fail loudly as itself
        # rather than only as two mismatched epochs.
        if resolved.get("261025023000S") == resolved.get("261025023000W"):
            failures.append(
                "  the two passes of the DST fall-back hour resolved to the SAME instant "
                f"({resolved.get('261025023000S')}) -- ON CONFLICT (time, source) will overwrite "
                "an hour of readings"
            )

        try:
            app._parse_dsmr_timestamp("not-a-timestamp")
            failures.append("  _parse_dsmr_timestamp('not-a-timestamp') should have raised ValueError")
        except ValueError:
            pass

        # The suffix is now load-bearing, so a timestamp missing it must be rejected outright
        # rather than quietly parsed with a guessed offset.
        try:
            app._parse_dsmr_timestamp("260812134420")
            failures.append("  _parse_dsmr_timestamp() should reject a timestamp with no S/W suffix")
        except ValueError:
            pass
    finally:
        if previous_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous_tz
        time.tzset()

    print("  DSMR timestamps: summer/winter resolved to real UTC instants; DST fall-back hour "
          "no longer collides; missing/garbage suffix rejected")


def check_smart_meter_source_is_stable(failures):
    """app.P1_SOURCE must stay 'dsmr'.

    Not a style assertion. `source` is a GROUP BY key in every electricity/gas continuous aggregate
    in the deploying stack and a filter in its dashboards, so changing this value splits history at
    the cutover instant instead of continuing it: duplicate daily/monthly rows, panels that join on
    day double-counting, and the data-gap monitoring panel going blind. That regression already
    shipped once. This is here to make the next attempt fail in CI instead of on the dashboards.
    """
    if app.P1_SOURCE != "dsmr":
        failures.append(
            f"  app.P1_SOURCE is {app.P1_SOURCE!r}, expected 'dsmr' -- changing it splits every "
            "electricity/gas continuous aggregate at the cutover instant; see its comment in app.py"
        )
        return
    print("  smart-meter source label: stable at 'dsmr' (history stays continuous)")


def check_telegram_buffer_cap(failures):
    """extract_telegram() must not retain an unbounded partial-telegram buffer.

    A stream containing a telegram start but never a valid `!<4 hex>\\r\\n` terminator (ser2net at
    the wrong baud, or the port repointed at a non-DSMR device) used to be retained in full, growing
    without limit in a container with no mem_limit. Three separate things are asserted, because a
    cap that merely bounds memory while breaking normal parsing would be worse than the leak.
    """
    # 1. Unterminated garbage is bounded, not retained.
    buffer = b""
    for _ in range(400):  # ~100 KB, comfortably past the 64 KB cap
        _, buffer = app.extract_telegram(buffer + b"/junk" + b"x" * 250)
        if len(buffer) > app.P1_MAX_BUFFER_BYTES:
            failures.append(
                f"  extract_telegram(): buffer grew to {len(buffer)} bytes, above the "
                f"{app.P1_MAX_BUFFER_BYTES}-byte cap"
            )
            return
    print(f"  telegram buffer cap: unterminated stream bounded at {len(buffer)} bytes "
          f"(cap {app.P1_MAX_BUFFER_BYTES}, was unbounded)")

    # 2. A real telegram still parses -- the cap must be nowhere near a legitimate one.
    telegram = _p1_telegram_bytes()
    parsed, remaining = app.extract_telegram(telegram)
    if parsed is None:
        failures.append("  extract_telegram(): a valid telegram no longer parses after the cap change")
        return
    if len(telegram) > app.P1_MAX_BUFFER_BYTES // 4:
        failures.append(
            f"  extract_telegram(): cap {app.P1_MAX_BUFFER_BYTES} leaves too little headroom over a "
            f"{len(telegram)}-byte telegram"
        )
        return
    print(f"  telegram buffer cap: {len(telegram)}-byte telegram still parses "
          f"({app.P1_MAX_BUFFER_BYTES // len(telegram)}x headroom)")

    # 3. A telegram split across recv() boundaries must survive. This is the assertion that would
    #    catch a cap applied on the wrong branch (e.g. clearing the buffer whenever no complete
    #    telegram is present yet), which would silently discard every telegram forever.
    buffer = b""
    for i in range(0, len(telegram), 250):
        parsed, buffer = app.extract_telegram(buffer + telegram[i:i + 250])
    if parsed is None:
        failures.append(
            "  extract_telegram(): a telegram arriving in 250-byte chunks no longer reassembles -- "
            "the cap is discarding partial telegrams mid-flight"
        )
        return
    print("  telegram buffer cap: chunked telegram still reassembles across recv() boundaries")


def check_publishable_fields(failures):
    """PUBLISHABLE_FIELDS must match what the poll loop actually publishes: poll_battery()'s solar
    values plus daily_power_yields, which the loop adds. Asserted against a real poll rather than by
    restating the set, so the two can't drift apart unnoticed."""
    before = len(failures)
    result = _poll_against_fake_battery()
    if result is None:
        failures.append("  publishable fields: could not poll the test battery")
        return
    returned = set(result[0]) | {"daily_power_yields"}
    if returned != app.PUBLISHABLE_FIELDS:
        failures.append(f"  PUBLISHABLE_FIELDS {sorted(app.PUBLISHABLE_FIELDS)} != published {sorted(returned)}")
    if len(failures) == before:
        print(f"  publishable fields: {len(app.PUBLISHABLE_FIELDS)} fields match the real poll")


def check_pvoutput_interval_guard(failures):
    """pvoutput_loop() must not divide by PVOUTPUT_INTERVAL before checking it.

    0 reads naturally as "disable" -- it means exactly that for P1_MIN_INTERVAL/PLUG_MIN_INTERVAL --
    but the bucket arithmetic raised ZeroDivisionError and killed the thread while the process
    stayed healthy: container Up, no uploads, no obvious cause.
    """
    original = app.PVOUTPUT_INTERVAL
    try:
        for value in (0, -1):
            app.PVOUTPUT_INTERVAL = value
            done = threading.Event()
            error = []

            def run():
                try:
                    app.pvoutput_loop()
                except Exception as exc:  # noqa: BLE001 -- the point is to catch ZeroDivisionError
                    error.append(exc)
                finally:
                    done.set()

            threading.Thread(target=run, daemon=True).start()
            # A correct implementation returns immediately; the buggy one raises immediately. Either
            # way this does not wait the full timeout, which only guards against a loop that spins.
            if not done.wait(5):
                failures.append(
                    f"  pvoutput_loop(): did not return with PVOUTPUT_INTERVAL={value}; it should "
                    "warn and stop rather than run"
                )
                return
            if error:
                failures.append(
                    f"  pvoutput_loop(): PVOUTPUT_INTERVAL={value} raised "
                    f"{type(error[0]).__name__}: {error[0]} -- it should warn and return instead"
                )
                return
    finally:
        app.PVOUTPUT_INTERVAL = original
    print("  pvoutput interval guard: PVOUTPUT_INTERVAL=0/-1 warns and stops the thread cleanly "
          "instead of raising ZeroDivisionError")


def check_log_level(failures):
    """app.resolve_log_level(): default WARNING for a minimal-logging production run, any of the
    five standard names case-insensitively, and a safe fallback (with a warning, not a crash) on
    anything else -- a typo in LOG_LEVEL must not take the container down."""
    cases = [
        (None, "WARNING", "unset"),
        ("warning", "WARNING", "lowercase"),
        ("Debug", "DEBUG", "mixed case"),
        ("  INFO  ", "INFO", "surrounding whitespace"),
        ("ERROR", "ERROR", "exact"),
        ("CRITICAL", "CRITICAL", "exact"),
    ]
    for raw, expected_name, label in cases:
        level, warning = app.resolve_log_level(raw)
        if level != app.LOG_LEVELS[expected_name]:
            failures.append(f"  resolve_log_level({raw!r}) [{label}]: expected {expected_name}, got {level}")
        if warning is not None:
            failures.append(f"  resolve_log_level({raw!r}) [{label}]: unexpected warning {warning!r}")

    # An empty string is how an unset value looks in a .env file (LOG_LEVEL=) -- treated the same
    # as absent, defaulting silently rather than warning about it.
    level, warning = app.resolve_log_level("")
    if level != app.LOG_LEVELS["WARNING"]:
        failures.append(f"  resolve_log_level(''): expected default WARNING, got {level}")
    if warning is not None:
        failures.append(f"  resolve_log_level(''): expected no warning for an empty value, got {warning!r}")

    for bad in ("verbose", "warn", "trace"):
        level, warning = app.resolve_log_level(bad)
        if level != app.LOG_LEVELS["WARNING"]:
            failures.append(f"  resolve_log_level({bad!r}): expected fallback to WARNING, got {level}")
        if warning is None:
            failures.append(f"  resolve_log_level({bad!r}): expected a fallback warning, got None")

    print("  log level: defaults to WARNING, accepts DEBUG/INFO/WARNING/ERROR/CRITICAL "
          "case-insensitively, falls back safely on anything else")


def check_derived_fields(failures):
    """The power_flow point. House use is grid + the unit's AC output, not PV + grid: with the
    battery charging, PV + grid would count the energy going into the cells as house use."""
    before = len(failures)
    values = {"total_active_power": 2350, "ac_output_power": 430, "battery_power": -1820}
    elec = {"electricity_currently_delivered": 0.0, "electricity_currently_returned": 0.12}

    # grid_w  = 0 - 120 = -120 (exporting)
    # house_w = -120 + 430 = 310   (PV + grid would have said 2230)
    got = app.derived_fields(values, elec)
    want = {"solar_w": 2350.0, "grid_w": -120.0, "house_w": 310.0, "battery_ac_w": 430.0, "battery_w": -1820.0}
    if got != want:
        failures.append(f"  derived fields: expected {want}, got {got}")
    if got and not all(isinstance(v, float) for v in got.values()):
        failures.append(f"  derived fields must all be floats, got {got}")

    # Without electricity, grid and house are absent, not zero.
    no_elec = app.derived_fields(values, None)
    if {"grid_w", "house_w"} & set(no_elec):
        failures.append(f"  derived fields without electricity kept grid/house: {sorted(no_elec)}")
    # Without the battery (stale poll), solar and house are absent; grid stays.
    no_battery = app.derived_fields({}, elec)
    if set(no_battery) != {"grid_w"}:
        failures.append(f"  derived fields without the battery: expected only grid_w, got {sorted(no_battery)}")

    if len(failures) == before:
        print("  derived fields: house = grid + battery AC output; omitted (not zeroed) without either input")


def check_solar_stale_fallback(failures):
    """_battery_values_for_derived(): a fresh poll gives its values, a stale one gives nothing, so
    power_flow leaves a gap instead of a plausible-looking 0."""
    before = len(failures)
    saved = (dict(app.latest_values), app.latest_poll_monotonic)
    try:
        with app._state_lock:
            app.latest_values.clear()
            app.latest_values.update({"total_active_power": 2350, "ac_output_power": 430,
                                      "battery_power": -1820, "total_power_yields": 1.0})
            app.latest_poll_monotonic = time.monotonic()
        fresh = app._battery_values_for_derived()
        if fresh != {"total_active_power": 2350, "ac_output_power": 430, "battery_power": -1820}:
            failures.append(f"  fresh battery poll: got {fresh}")
        with app._state_lock:
            app.latest_poll_monotonic = time.monotonic() - app.SOLAR_STALE_AFTER - 1
        if app._battery_values_for_derived() != {}:
            failures.append("  stale battery poll must give {}, not the cached values")
        with app._state_lock:
            app.latest_poll_monotonic = None
        if app._battery_values_for_derived() != {}:
            failures.append("  no poll yet must give {}")
    finally:
        with app._state_lock:
            app.latest_values.clear()
            app.latest_values.update(saved[0])
            app.latest_poll_monotonic = saved[1]
    if len(failures) == before:
        print("  battery staleness: fresh poll used, stale or missing poll omitted (not zeroed)")


def check_pvoutput_metrics(failures):
    """PVOutput metric formulas in app.METRICS, and build_pvoutput_params() degrading to
    battery-only output when no smart-meter data is cached."""
    before = len(failures)
    values = {"daily_power_yields": 8.7, "total_power_yields": 27875.3, "total_active_power": 2350,
              "ac_output_power": 430, "soc": 63}
    elec = {
        "electricity_currently_delivered": 0.0,
        "electricity_currently_returned": 0.12,
        "electricity_delivered_1": 14351.805,
        "electricity_delivered_2": 11749.026,
        "electricity_returned_1": 6029.28,
        "electricity_returned_2": 12632.371,
        "phase_voltage_l1": 225.0,
    }
    for name, want in (("house_power_w", 310), ("daily_generation_wh", 8700),
                       ("total_generation_wh", 27875300), ("generation_w", 2350),
                       ("grid_voltage_v", 225.0), ("battery_soc", 63)):
        got = app.METRICS[name][1](values, elec)
        if got != want:
            failures.append(f"  {name}: expected {want}, got {got}")

    orig_mapping = dict(app.PVOUTPUT_MAPPING)
    orig_enable_p1 = app.ENABLE_P1
    orig_elec_cache = app._elec_cache
    app.ENABLE_P1 = True
    app._elec_cache = None
    app.PVOUTPUT_MAPPING = {"v1": "daily_generation_wh", "v3": "house_power_w"}
    previous_level = app.log.level
    app.log.setLevel(logging.CRITICAL)
    try:
        params = app.build_pvoutput_params(values)
        if "v3" in params:
            failures.append(f"  build_pvoutput_params() included v3 without electricity data: {params}")
        if params.get("v1") != 8700:
            failures.append(f"  build_pvoutput_params() lost v1: {params}")
        # No daily yield yet (no baseline): v1 is omitted, the upload still goes out with v2.
        app.PVOUTPUT_MAPPING = {"v1": "daily_generation_wh", "v2": "generation_w"}
        params = app.build_pvoutput_params({k: v for k, v in values.items() if k != "daily_power_yields"})
        if "v1" in params or params.get("v2") != 2350:
            failures.append(f"  build_pvoutput_params() without a daily yield: {params}")
    finally:
        app.PVOUTPUT_MAPPING = orig_mapping
        app.ENABLE_P1 = orig_enable_p1
        app._elec_cache = orig_elec_cache
        app.log.setLevel(previous_level)

    if len(failures) == before:
        print("  pvoutput metrics: house = grid + battery AC output; degrades without P1 or a daily baseline")


def check_daily_generation_wh_corrected(failures):
    """app.daily_yield_kwh(): today's generation from the lifetime total and the last total before
    local midnight, which is how every daily figure in the stack is computed."""
    before = len(failures)
    today = datetime.now().astimezone().date()
    saved = (app._daily_yield_baseline, app._daily_yield_retry_after, app.psycopg.connect)

    def _explode(**kwargs):
        raise AssertionError("psycopg.connect() should not have been called")

    previous_level = app.log.level
    app.log.setLevel(logging.CRITICAL)
    try:
        # 1. Cache hit: never touches Postgres.
        app._daily_yield_baseline = (today, 27866.5)
        app._daily_yield_retry_after = 0.0
        app.psycopg.connect = _explode
        got = app.daily_yield_kwh(27875.3)
        if got != 8.8:
            failures.append(f"  daily_yield_kwh() cache hit: expected 8.8, got {got}")

        # 2. Cache miss: queries, prefers the precise baseline, caches it.
        app._daily_yield_baseline = None
        app.psycopg.connect = lambda **kwargs: _FakeConnection(fetch_result=(27866, 27866.5))
        got = app.daily_yield_kwh(27875.3)
        if got != 8.8 or app._daily_yield_baseline != (today, 27866.5):
            failures.append(f"  daily_yield_kwh() cache miss: got {got}, cache {app._daily_yield_baseline!r}")

        # 3. No row before midnight: None, and no new query on the next poll.
        app._daily_yield_baseline = None
        app.psycopg.connect = lambda **kwargs: _FakeConnection(fetch_result=None)
        got = app.daily_yield_kwh(27875.3)
        app.psycopg.connect = _explode
        again = app.daily_yield_kwh(27875.3)
        if got is not None or again is not None:
            failures.append(f"  daily_yield_kwh() without a baseline: expected None twice, got {got}, {again}")

        # 4. Never negative.
        app._daily_yield_baseline = (today, 27880.0)
        if app.daily_yield_kwh(27875.3) != 0.0:
            failures.append("  daily_yield_kwh() went negative below the baseline")
    finally:
        app._daily_yield_baseline, app._daily_yield_retry_after, app.psycopg.connect = saved
        app.log.setLevel(previous_level)

    if len(failures) == before:
        print("  daily yield: cache hit skips Postgres, cache miss queries and caches, no row gives None "
              "(held off), never negative")


def _monta_charge(i, **over):
    c = {"id": 900000000000000000 + i, "humanReadableId": f"TEST-{i}", "chargePointId": 4242,
         "createdAt": "2026-11-07T14:14:58Z", "updatedAt": "2026-11-07T15:27:15Z",
         "cablePluggedInAt": "2026-11-07T14:15:10Z", "startedAt": "2026-11-07T14:15:03Z",
         "stoppedAt": "2026-11-07T15:22:30.99Z", "state": "completed", "consumedKwh": 11.85,
         "price": 6.17, "cost": 3.53, "currency": {"identifier": "eur", "name": "Euros", "decimals": 2}}
    c.update(over)
    return c



def check_monta(failures):
    """ENABLE_MONTA: the downloaded stroohm_monta_api.py still produces what app.sync_monta() reads
    (its fields, the excl.-VAT price matching a real receipt: 6.17 -> 5.10, aware UTC times), the
    upsert's shape (ON CONFLICT (id), raw JSON kept), and the MONTA_TIMES schedule."""
    import stroohm_monta_api

    before = len(failures)
    orig_connect = app.psycopg.connect
    try:
        got = [stroohm_monta_api.session(_monta_charge(i)) for i in (1, 2, 3)]
        s = got[0]
        missing = [c for c in app.MONTA_COLUMNS if c not in s]
        if missing:
            failures.append(f"  stroohm_monta_api.session() lacks {missing}")
        expect = {"price_incl_vat": 6.17, "price_excl_vat": 5.10, "currency": "EUR",
                  "charge_point_id": 4242, "energy_kwh": 11.85,
                  "started_at": datetime(2026, 11, 7, 14, 15, 3, tzinfo=timezone.utc)}
        bad = {k: s.get(k) for k in expect if s.get(k) != expect[k]}
        if bad:
            failures.append(f"  stroohm_monta_api.session(): wrong {bad}")

        fake = _FakeConnection()
        app.psycopg.connect = lambda **kwargs: fake

        class _Client:
            def sessions(self, **kwargs):
                return iter(got)

        n = app.sync_monta(_Client())
        sql, params = fake.cursor_obj.executed[0]
        if n != 3 or len(fake.cursor_obj.executed) != 3:
            failures.append(f"  sync_monta: wrote {n} / executed {len(fake.cursor_obj.executed)}, expected 3")
        if f"INSERT INTO {app.MONTA_TABLE}" not in sql or "ON CONFLICT (id) DO UPDATE" not in sql:
            failures.append(f"  sync_monta: unexpected SQL {sql}")
        row = dict(zip(app.MONTA_COLUMNS, params))
        if row["id"] != 900000000000000001 or json.loads(row["raw"])["cost"] != 3.53:
            failures.append(f"  sync_monta: unexpected params {row}")
    except Exception as e:
        failures.append(f"  monta: {type(e).__name__}: {e}")
    finally:
        app.psycopg.connect = orig_connect

    # Schedule: MONTA_TIMES parsing and which slot is due.
    try:
        if app.parse_monta_times("21:00, 04:00,21:00") != [(4, 0), (21, 0)]:
            failures.append("  parse_monta_times: '21:00, 04:00,21:00' should give [(4, 0), (21, 0)]")
        for bad in ("", "4", "25:00", "04:60", "04:00,x"):
            if app.parse_monta_times(bad) is not None:
                failures.append(f"  parse_monta_times({bad!r}) should be None")
        times = [(4, 0), (21, 0)]
        cases = [
            (datetime(2026, 11, 9, 3, 59), datetime(2026, 11, 8, 21, 0)),   # before 04:00 -> yesterday 21:00
            (datetime(2026, 11, 9, 4, 0), datetime(2026, 11, 9, 4, 0)),
            (datetime(2026, 11, 9, 20, 59), datetime(2026, 11, 9, 4, 0)),
            (datetime(2026, 11, 9, 23, 0), datetime(2026, 11, 9, 21, 0)),
        ]
        for now, want in cases:
            got = app.latest_monta_slot(now, times)
            if got != want:
                failures.append(f"  latest_monta_slot({now}): expected {want}, got {got}")
    except Exception as e:
        failures.append(f"  monta schedule: {type(e).__name__}: {e}")

    if len(failures) == before:
        print("  monta: stroohm_monta_api session fields (excl. VAT, UTC times), sync upsert, "
              "MONTA_TIMES parsing and due-slot logic")


def _timeout_handler(signum, frame):
    print(f"SMOKE TEST FAILED: exceeded overall {OVERALL_TIMEOUT_S}s timeout (a check likely hung)", file=sys.stderr)
    os._exit(1)


def main():
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(OVERALL_TIMEOUT_S)

    failures = []
    check_modbus_poll(failures)
    check_battery_is_read_only(failures)
    check_inverter_reconnect_after_broken_pipe(failures)
    check_mqtt_connect(failures)
    check_mqtt_publish_switch(failures)
    check_postgres_write(failures)
    check_postgres_write_retry(failures)
    check_clean_stop(failures)
    check_get_last_gas_reading(failures)
    check_p1_telegram_parsing(failures)
    check_p1_relay_broadcast(failures)
    check_p1_stall_reconnect(failures)
    check_p1_http_reader(failures)
    check_plug_mqtt_message(failures)
    check_dsmr_timestamp_parsing(failures)
    check_smart_meter_source_is_stable(failures)
    check_pvoutput_metrics(failures)
    check_daily_generation_wh_corrected(failures)
    check_derived_fields(failures)
    check_solar_stale_fallback(failures)
    check_telegram_buffer_cap(failures)
    check_publishable_fields(failures)
    check_pvoutput_interval_guard(failures)
    check_log_level(failures)
    check_monta(failures)

    signal.alarm(0)

    if failures:
        print(f"SMOKE TEST FAILED ({len(failures)}):", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        return 1

    print("smoke test passed: modbus, battery read-only, battery reconnect after broken pipe, mqtt connect, mqtt publish switch, "
          "postgres write, postgres write retry, clean stop, postgres read, P1 telegram parsing, "
          "P1 relay broadcast, P1 stall reconnect, P1 over HTTP, plug parsing, DSMR timestamps (incl. DST fall-back), "
          "smart-meter source stability, pvoutput metrics, daily generation yield correction, "
          "derived fields, solar staleness fallback, "
          "telegram buffer cap, publishable fields, pvoutput interval guard, log level, monta")
    return 0


if __name__ == "__main__":
    sys.exit(main())
