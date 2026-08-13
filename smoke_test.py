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
import os
import signal
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
    "1-0:32.7.0(235.0*V)\r\n"
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
    }
    expected_elec_ts_ns = app._parse_dsmr_timestamp("260812134420S")
    expected_gas_ts_ns = app._parse_dsmr_timestamp("260812134400S")

    elec, elec_ts_ns, gas = app.parse_p1_telegram(telegram)
    if elec != expected_elec:
        failures.append(f"  parse_p1_telegram() electricity: expected {expected_elec}, got {elec}")
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
        elif elec_item[0] != expected_elec or elec_item[3] != "p1":
            failures.append(f"  queued elec point: expected ({expected_elec}, source='p1'), got {elec_item}")

        if gas_item is None:
            failures.append(f"  no point enqueued for gas table {app.GAS_TABLE!r}; queued={queued}")
        elif gas_item[0] != {"delivered": 6573.284} or gas_item[3] != "p1":
            failures.append(f"  queued gas point: expected (delivered=6573.284, source='p1'), got {gas_item}")

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
    """app._parse_dsmr_timestamp() on DSMR's YYMMDDhhmmss+[SW] format (both the summer and winter
    suffix), computed against Python's own local-time resolution for the same calendar date --
    not a hardcoded epoch value, since the S/W suffix is deliberately ignored in favour of the
    container's own TZ (see the function's docstring) -- plus rejection of a malformed timestamp.
    """
    for raw, label in (("260812134420S", "summer/DST suffix"), ("260101090000W", "winter suffix")):
        year, month, day, hour, minute, second = 2000 + int(raw[0:2]), int(raw[2:4]), int(raw[4:6]), \
            int(raw[6:8]), int(raw[8:10]), int(raw[10:12])
        expected = int(datetime(year, month, day, hour, minute, second).astimezone().timestamp() * 1_000_000_000)
        got = app._parse_dsmr_timestamp(raw)
        if got != expected:
            failures.append(f"  _parse_dsmr_timestamp({raw!r}) [{label}]: expected {expected}, got {got}")

    try:
        app._parse_dsmr_timestamp("not-a-timestamp")
        failures.append("  _parse_dsmr_timestamp('not-a-timestamp') should have raised ValueError")
    except ValueError:
        pass

    print("  DSMR timestamp parsing: summer/winter suffix both resolved via local TZ; garbage rejected")


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
    """The inverter goes fully offline overnight (Modbus stops responding), which used to mean
    power_flow stopped updating entirely until the next successful poll. Covers both halves of
    the fix: _solar_power_for_derived() falling back to 0 once the last poll is too old, and a P1
    telegram triggering its own power_flow write so house/grid keep updating regardless (already
    exercised end to end in check_p1_telegram_parsing -- this just covers the staleness half).
    """
    before = len(failures)
    orig_latest_values = dict(app.latest_values)
    orig_latest_poll_monotonic = app.latest_poll_monotonic

    try:
        # Fresh poll -> the real value.
        with app._state_lock:
            app.latest_values.clear()
            app.latest_values["total_active_power"] = 2500.0
            app.latest_poll_monotonic = time.monotonic()
        got = app._solar_power_for_derived()
        if got != 2500.0:
            failures.append(f"  solar power with a fresh poll: expected 2500.0, got {got!r}")

        # Poll older than SOLAR_STALE_AFTER -> 0, not the stale reading.
        with app._state_lock:
            app.latest_poll_monotonic = time.monotonic() - app.SOLAR_STALE_AFTER - 1
        got = app._solar_power_for_derived()
        if got != 0.0:
            failures.append(f"  solar power with a stale poll: expected 0.0, got {got!r}")

        # No poll at all yet (e.g. container just started) -> 0.
        with app._state_lock:
            app.latest_values.clear()
            app.latest_poll_monotonic = None
        got = app._solar_power_for_derived()
        if got != 0.0:
            failures.append(f"  solar power with no poll yet: expected 0.0, got {got!r}")

        if len(failures) == before:
            print("  solar staleness fallback: falls back to 0 W once the last poll is too old")
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
    check_postgres_write(failures)
    check_get_last_gas_reading(failures)
    check_p1_telegram_parsing(failures)
    check_p1_relay_broadcast(failures)
    check_plug_mqtt_message(failures)
    check_dsmr_timestamp_parsing(failures)
    check_pvoutput_metrics(failures)
    check_derived_fields(failures)
    check_solar_stale_fallback(failures)
    check_log_level(failures)

    signal.alarm(0)

    if failures:
        print(f"SMOKE TEST FAILED ({len(failures)}):", file=sys.stderr)
        print("\n".join(failures), file=sys.stderr)
        return 1

    print("smoke test passed: modbus, mqtt connect, postgres write, postgres read, P1 telegram parsing, "
          "P1 relay broadcast, plug parsing, DSMR timestamps, pvoutput metrics, derived fields, "
          "solar staleness fallback, log level")
    return 0


if __name__ == "__main__":
    sys.exit(main())
