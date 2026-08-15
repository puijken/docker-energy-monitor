"""Smoke test: exercise the app's three external dependencies without any real service.

Exists because building the image proves nothing about runtime. A dependency bump that renames a
pymodbus argument, changes a datastore class or alters register decoding still builds cleanly and
only fails when it first talks to hardware -- which has already happened once (pymodbus 3.14
dropped the `slave` keyword in favour of `device_id`).

Originally this only covered pymodbus (start an in-process Modbus server, poll it through
app.poll_inverter, check the decoded values). Dependabot auto-merges non-major bumps to
paho-mqtt, requests and psycopg too, so an API change in any of them would also pass CI and only
surface at runtime. This file now additionally covers, all without a real broker or Postgres:

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
    inverter-only output when no electricity data is cached.

Run with no arguments; exits non-zero on the first failed expectation. Guarded by an overall
wall-clock timeout so a hang in any check (e.g. a client that doesn't disconnect cleanly) fails
loudly instead of stalling CI.
"""
import json
import logging
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone
from queue import Empty

os.environ.setdefault("INVERTER_HOST", "127.0.0.1")
os.environ.setdefault("INVERTER_PORT", "15020")
os.environ.setdefault("POSTGRES_HOST", "127.0.0.1")
os.environ.setdefault("POSTGRES_USER", "smoke")
os.environ.setdefault("POSTGRES_PASSWORD", "smoke")
os.environ.setdefault("MQTT_HOST", "127.0.0.1")

from pymodbus.datastore import ModbusSequentialDataBlock, ModbusServerContext  # noqa: E402
from pymodbus.server import StartTcpServer  # noqa: E402

import app  # noqa: E402
import paho.mqtt.client as mqtt  # noqa: E402

PORT = int(os.environ["INVERTER_PORT"])

OVERALL_TIMEOUT_S = 60

# Raw register values, indexed by wire address (protocol address - 1), chosen so every decode path
# is exercised: U16 with scaling, S16, and U32 low/high word assembly.
RAW = {
    5002: 83,       # daily_power_yields   U16 x0.1 -> 8.3 kWh
    5003: 26261,    # total_power_yields   U32 low
    5004: 0,        #                      U32 high -> 26261 kWh (unscaled)
    5005: 18173,    # total_running_time   U32 low
    5006: 0,        #                      U32 high -> 18173 h
    5007: 571,      # internal_temperature S16 x0.1 -> 57.1 C
    5008: 2990,     # total_apparent_power U32 low
    5009: 0,        #                      U32 high -> 2990 VA
    5010: 2567,     # mppt_1_voltage       -> 256.7 V
    5011: 29,       # mppt_1_current       -> 2.9 A
    5012: 2317,     # mppt_2_voltage       -> 231.7 V
    5013: 95,       # mppt_2_current       -> 9.5 A
    5016: 2968,     # total_dc_power       U32 low
    5017: 0,        #                      U32 high -> 2968 W
    5018: 2378,     # phase_a_voltage      -> 237.8 V
    5021: 122,      # phase_a_current      S16 x0.1 -> 12.2 A
    5030: 2888,     # total_active_power   U32 low
    5031: 0,        #                      U32 high -> 2888 W
    5037: 0x0000,   # work_state_1         U16 -> 0x0000 "Run"
    5143: 471,      # fine total yield     U32 low  (FINE_TOTAL_YIELD_REGISTER, polled separately
    5144: 4,        #                      U32 high  from the block above) -> 26261.5 kWh x0.1
}
EXPECTED = {
    "daily_power_yields": 8.3,
    "total_power_yields": 26261,
    "total_running_time": 18173,
    "internal_temperature": 57.1,
    "total_apparent_power": 2990,
    "mppt_1_voltage": 256.7,
    "mppt_1_current": 2.9,
    "mppt_2_voltage": 231.7,
    "mppt_2_current": 9.5,
    "total_dc_power": 2968,
    "phase_a_voltage": 237.8,
    "phase_a_current": 12.2,
    "total_active_power": 2888,
    "run_state": "Run",
    "total_power_yields_precise": 26261.5,
}


def serve():
    """Serve RAW over Modbus TCP.

    Only the test harness uses pymodbus's server API, which is the volatile part: 3.14 renamed
    ModbusSlaveContext to ModbusDeviceContext, and v4 will drop these in favour of SimData/SimDevice
    (hence the deprecation warnings). If a bump breaks *this* function rather than an assertion, the
    harness needs updating, not the app.
    """
    regs = [0] * (max(RAW) + 8)
    for address, value in RAW.items():
        regs[address] = value
    # ModbusSequentialDataBlock stores at address-1, so 1 maps regs[i] to wire address i.
    block = ModbusSequentialDataBlock(1, regs)
    try:  # pymodbus >= 3.14
        from pymodbus.datastore import ModbusDeviceContext

        context = ModbusServerContext(devices=ModbusDeviceContext(ir=block), single=True)
    except ImportError:  # older pymodbus
        from pymodbus.datastore import ModbusSlaveContext

        context = ModbusServerContext(slaves=ModbusSlaveContext(ir=block, zero_mode=True), single=True)
    StartTcpServer(context=context, address=("127.0.0.1", PORT))


def check_modbus_poll(failures):
    """Original coverage: decode registers through app.poll_inverter() against a real pymodbus
    server, plus the physical-invariant and line-protocol-formatting checks that rely on it."""
    threading.Thread(target=serve, daemon=True).start()

    from pymodbus.client import ModbusTcpClient

    client = ModbusTcpClient("127.0.0.1", port=PORT, timeout=5)
    time.sleep(0.5)  # let the server bind first, so the log isn't cluttered with a refused connect
    for _ in range(30):
        if client.connect():
            break
        time.sleep(0.5)
    else:
        failures.append("  could not connect to the test inverter")
        return

    values = app.poll_inverter(client)
    client.close()

    for name, want in EXPECTED.items():
        got = values.get(name)
        if got != want:
            failures.append(f"  {name}: expected {want!r}, got {got!r}")

    # Physical consistency: DC input must cover AC output, and the MPPT strings must account for
    # the DC total. Catches sign/word-order/scaling regressions that individual values might hide.
    dc, ac = values["total_dc_power"], values["total_active_power"]
    mppt = (
        values["mppt_1_voltage"] * values["mppt_1_current"]
        + values["mppt_2_voltage"] * values["mppt_2_current"]
    )
    if not ac <= dc:
        failures.append(f"  AC output {ac}W exceeds DC input {dc}W")
    if abs(mppt - dc) / dc > 0.05:
        failures.append(f"  MPPT sum {mppt:.0f}W disagrees with total_dc_power {dc}W by >5%")

    # Apparent power can never be less than active power, and volts times amps on the AC side has
    # to land back on active power. Both catch a word-order or scaling slip on the registers added
    # later than the original block, which a single-value comparison would not.
    va = values["total_apparent_power"]
    if va < ac:
        failures.append(f"  apparent power {va}VA is below active power {ac}W")
    ac_from_vi = values["phase_a_voltage"] * values["phase_a_current"]
    if abs(ac_from_vi - ac) / ac > 0.05:
        failures.append(f"  phase A V*I {ac_from_vi:.0f}W disagrees with active power {ac}W by >5%")

    # FINE_TOTAL_YIELD_REGISTER is a separate round trip against the same lifetime counter as
    # total_power_yields, just 0.1 kWh-scaled -- the two must agree to within the coarse register's
    # own 1 kWh rounding, or a word-order/scale slip on the new register would silently ship.
    coarse, fine = values["total_power_yields"], values.get("total_power_yields_precise")
    if fine is None:
        failures.append("  total_power_yields_precise missing (fine-yield register read failed)")
    elif abs(fine - coarse) > 1:
        failures.append(f"  fine total yield {fine}kWh disagrees with coarse {coarse}kWh by >1kWh")

    print(f"  modbus poll: {len(EXPECTED)} values decoded, unit kwarg={app._UNIT_KWARG!r}")


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
            {"total_active_power": 2888.0, "run_state": "ON", "not_a_real_column": 1.5},
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
    if params[3:] != [2888.0, "ON"]:
        failures.append(f"  write_postgres() known-column params: expected [2888.0, 'ON'], got {params[3:]!r}")

    if len(failures) == before:
        print("  postgres write: SQL/ON CONFLICT/params as expected, unmapped field routed into `extra`")


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
    """The power_flow point written on each poll, which exists so Grafana never has to join
    measurements. Reuses the same inverter/electricity fixtures as the metric checks below."""
    before = len(failures)
    inverter = {"total_active_power": 2888}
    elec = {
        "electricity_currently_delivered": 0.0,
        "electricity_currently_returned": 1.644,
    }

    # solar_w = 2888
    # grid_w  = (0.0 * 1000) - (1.644 * 1000) = -1644   (negative == feeding the grid)
    # house_w = 2888 + (0.0 - 1.644) * 1000  =  1244
    got = app.derived_fields(inverter, elec)
    for name, want in (("solar_w", 2888.0), ("grid_w", -1644.0), ("house_w", 1244.0)):
        if got.get(name) != want:
            failures.append(f"  derived {name}: expected {want}, got {got.get(name)!r}")
    if got and not all(isinstance(v, float) for v in got.values()):
        failures.append(f"  derived fields must all be floats, got {got}")

    # Without cached electricity data the grid/house fields must be absent, not zero -- a zero
    # would render in Grafana as a real "house using nothing" reading.
    solar_only = app.derived_fields(inverter, None)
    if set(solar_only) != {"solar_w"}:
        failures.append(f"  derived fields without electricity: expected only solar_w, got {sorted(solar_only)}")

    if len(failures) == before:
        print("  derived fields: solar/grid/house computed together; omitted (not zeroed) without electricity")


def check_solar_stale_fallback(failures):
    """A stale solar poll is ambiguous, and _solar_power_for_derived() must resolve it from the
    state the inverter was last seen in rather than by substituting a fixed 0.

    It used to always substitute 0, justified by the inverter going offline overnight. It doesn't:
    it stays reachable and reports `Standby` at exactly 0 W all night, so nights never depended on
    the substitution -- it only ever fired on a genuine poll failure, which is exactly when 0 W is
    wrong. The last case below is the one that matters: a producing array reported as idle makes an
    exporting house compute as *negative* consumption.
    """
    before = len(failures)
    orig_latest_values = dict(app.latest_values)
    orig_latest_poll_monotonic = app.latest_poll_monotonic

    def _set(power, run_state, age):
        with app._state_lock:
            app.latest_values.clear()
            if power is not None:
                app.latest_values["total_active_power"] = power
            if run_state is not None:
                app.latest_values["run_state"] = run_state
            app.latest_poll_monotonic = None if age is None else time.monotonic() - age

    try:
        # Fresh poll -> the real value, whatever the state says.
        _set(2500.0, "Run", 0)
        got = app._solar_power_for_derived()
        if got != 2500.0:
            failures.append(f"  fresh poll: expected 2500.0, got {got!r}")

        # Stale, but last seen idle -> a real 0. This is the night/shutdown case, and the only one
        # in which substituting zero was ever justified.
        for state in sorted(app.IDLE_RUN_STATES):
            _set(0.0, state, app.SOLAR_STALE_AFTER + 1)
            got = app._solar_power_for_derived()
            if got != 0.0:
                failures.append(f"  stale poll, last seen {state!r} (idle): expected 0.0, got {got!r}")

        # Stale, but last seen producing -> unknown. Must NOT claim 0.
        _set(3000.0, "Run", app.SOLAR_STALE_AFTER + 1)
        got = app._solar_power_for_derived()
        if got is not None:
            failures.append(
                f"  stale poll, last seen producing at 3000 W: expected None (unknown), got {got!r} "
                "-- reporting a producing array as idle makes house_w go negative"
            )

        # An unmapped/unknown state is not on the idle list, so it must also decline to guess.
        _set(3000.0, "Unknown (0x1234)", app.SOLAR_STALE_AFTER + 1)
        if app._solar_power_for_derived() is not None:
            failures.append("  stale poll with an unmapped run_state should be None, not 0.0")

        # No poll at all yet (container just started) -> unknown, not zero.
        _set(None, None, None)
        got = app._solar_power_for_derived()
        if got is not None:
            failures.append(f"  no poll yet: expected None, got {got!r}")

        # And the consequence end to end: an unknown solar reading drops solar_w and house_w but
        # keeps grid_w, which comes straight off the meter.
        elec = {"electricity_currently_delivered": 0.0, "electricity_currently_returned": 0.5}
        flow = app.derived_fields({"total_active_power": None}, elec)
        if "solar_w" in flow or "house_w" in flow:
            failures.append(f"  derived_fields with unknown solar should omit solar_w/house_w, got {flow}")
        elif flow.get("grid_w") != -500.0:
            failures.append(f"  derived_fields with unknown solar should still report grid_w=-500.0, got {flow}")

        # The old behaviour, for contrast: 0 W here would have produced house_w = -500 W.
        old = app.derived_fields({"total_active_power": 0.0}, elec)
        if old.get("house_w") != -500.0:
            failures.append(
                f"  sanity check failed: substituting 0 W should still compute house_w=-500.0, got {old}"
            )

        if len(failures) == before:
            print("  solar staleness: resolved from the last run_state -- real 0 when idle, omitted "
                  "(not zeroed) when it was producing, so house_w can't go negative")
    finally:
        with app._state_lock:
            app.latest_values.clear()
            app.latest_values.update(orig_latest_values)
            app.latest_poll_monotonic = orig_latest_poll_monotonic


def check_pvoutput_metrics(failures):
    """PVOutput metric formulas in app.METRICS, and build_pvoutput_params() degrading to
    inverter-only output when no smart-meter data is cached."""
    before = len(failures)
    inverter = {
        "daily_power_yields": 8.3,
        "total_power_yields": 26261,
        "total_active_power": 2888,
        "total_dc_power": 2968,
        "phase_a_voltage": 237.8,
        "internal_temperature": 57.1,
    }
    elec = {
        "electricity_currently_delivered": 0.0,
        "electricity_currently_returned": 1.644,
        "electricity_delivered_1": 13658.117,
        "electricity_delivered_2": 11190.901,
        "electricity_returned_1": 5750.043,
        "electricity_returned_2": 12043.452,
        "phase_voltage_l1": 235.0,
    }

    # house_power_w = total_active_power + (currently_delivered - currently_returned) * 1000
    #               = 2888 + (0.0 - 1.644) * 1000 = 2888 - 1644 = 1244
    expected_house_power_w = 1244
    got_house_power_w = app.METRICS["house_power_w"][1](inverter, elec)
    if got_house_power_w != expected_house_power_w:
        failures.append(f"  house_power_w: expected {expected_house_power_w}, got {got_house_power_w}")

    # total_house_energy_wh = total_power_yields*1000 + (delivered_1+delivered_2)*1000
    #                                                  - (returned_1+returned_2)*1000
    #   delivered_1+delivered_2 = 13658.117 + 11190.901 = 24849.018 -> *1000 = 24849018
    #   returned_1+returned_2  =  5750.043 + 12043.452  = 17793.495 -> *1000 = 17793495
    #   total = 26261*1000 + 24849018 - 17793495 = 26261000 + 24849018 - 17793495 = 33316523
    expected_total_house_energy_wh = 33316523
    got_total_house_energy_wh = app.METRICS["total_house_energy_wh"][1](inverter, elec)
    if got_total_house_energy_wh != expected_total_house_energy_wh:
        failures.append(
            f"  total_house_energy_wh: expected {expected_total_house_energy_wh}, got {got_total_house_energy_wh}"
        )

    # build_pvoutput_params() must drop parameters that need electricity data when nothing is
    # cached, rather than erroring out or reporting stale figures.
    orig_mapping = dict(app.PVOUTPUT_MAPPING)
    orig_enable_p1 = app.ENABLE_P1
    orig_elec_cache = app._elec_cache
    app.ENABLE_P1 = True
    app._elec_cache = None
    app.PVOUTPUT_MAPPING = {"v1": "daily_generation_wh", "v3": "house_power_w"}
    try:
        params = app.build_pvoutput_params(inverter)
        if "v3" in params:
            failures.append(
                f"  build_pvoutput_params() included v3 (house_power_w needs P1 electricity data) "
                f"with no cached reading: {params}"
            )
        if "v1" not in params:
            failures.append(f"  build_pvoutput_params() dropped v1 (inverter-only) unexpectedly: {params}")
    finally:
        app.PVOUTPUT_MAPPING = orig_mapping
        app.ENABLE_P1 = orig_enable_p1
        app._elec_cache = orig_elec_cache

    if len(failures) == before:
        print(
            "  pvoutput metrics: house_power_w/total_house_energy_wh correct; "
            "degrades to inverter-only without P1 electricity data"
        )


def _timeout_handler(signum, frame):
    print(f"SMOKE TEST FAILED: exceeded overall {OVERALL_TIMEOUT_S}s timeout (a check likely hung)", file=sys.stderr)
    os._exit(1)


def main():
    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(OVERALL_TIMEOUT_S)

    failures = []
    check_modbus_poll(failures)
    check_mqtt_connect(failures)
    check_mqtt_publish_switch(failures)
    check_postgres_write(failures)
    check_get_last_gas_reading(failures)
    check_p1_telegram_parsing(failures)
    check_p1_relay_broadcast(failures)
    check_p1_stall_reconnect(failures)
    check_plug_mqtt_message(failures)
    check_dsmr_timestamp_parsing(failures)
    check_smart_meter_source_is_stable(failures)
    check_pvoutput_metrics(failures)
    check_derived_fields(failures)
    check_solar_stale_fallback(failures)
    check_telegram_buffer_cap(failures)
    check_log_level(failures)

    signal.alarm(0)

    if failures:
        print(f"SMOKE TEST FAILED ({len(failures)}):", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        return 1

    print("smoke test passed: modbus, mqtt connect, mqtt publish switch, postgres write, postgres read, P1 telegram parsing, "
          "P1 relay broadcast, P1 stall reconnect, plug parsing, DSMR timestamps (incl. DST fall-back), "
          "smart-meter source stability, pvoutput metrics, derived fields, solar staleness fallback, "
          "log level")
    return 0


if __name__ == "__main__":
    sys.exit(main())
