"""
opc_tdengine_collector.py

Scalable OPC UA → TDengine collector:
- Single asyncio loop with one OPC UA client
- Multiple subscriptions (one per sensor)
- Shared buffer with periodic batch flush
- Small connection pool for TDengine writes
"""

import asyncio
from datetime import datetime
from asyncua import Client
# import taos
# import taosws
import requests
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from dataclasses import dataclass
import signal

# -------------------------
# Configuration
# -------------------------
OPC_UA_SERVER = "opc.tcp://localhost:4840"
# TD_CONNECTION = "taosws://root:taosdata@localhost:6030"
# TD_CONNECTION = "taosws://root:taosdata@localhost:6041"
# TD_CONNECTION = "taosws://root:taosdata@localhost:6041/rest/sql"
TD_REST_URL = "http://localhost:6041/rest/sql"
TD_USER = "root"
TD_PASSWORD = "taosdata"
DB_NAME = "sensor_db"

# Sensor definitions: (opc_node_id, child_table_name)
SENSORS = [
    ("ns=2;s=Temperature", "sensor1"),
    ("ns=2;s=Humidity", "sensor2"),
    ("ns=2;s=Pressure", "sensor3"),
    ("ns=2;s=Viscocity", "sensor4"),
    ("ns=2;s=OutsideTemperature", "sensor5"),
    ("ns=2;s=Station1Pressure", "sensor7"),
    ("ns=2;s=Station2Pressure", "sensor8"),
    ("ns=2;s=Station3Pressure", "sensor9"),
    ("ns=2;s=Station4Pressure", "sensor10"),
    ("ns=2;s=Station5Pressure", "sensor11"),
    ("ns=2;s=Station6Pressure", "sensor12"),
    ("ns=2;s=Station7Pressure", "sensor13"),
    ("ns=2;s=Station8Pressure", "sensor14"),
    ("ns=2;s=Station9Pressure", "sensor15"),
    ("ns=2;s=Station10Pressure", "sensor16"),
    ("ns=2;s=Voltage", "sensor17")
]

FLUSH_INTERVAL_SEC = 1.0      # Flush every N seconds
FLUSH_ROW_THRESHOLD = 500     # Or when total rows exceed this
DB_POOL_SIZE = 4              # Number of TDengine connections


# -------------------------
# Connection Pool (REST-based)
# -------------------------
class TDenginePool:
    """TDengine REST API client."""

    def __init__(self, rest_url: str, user: str, password: str, db_name: str, size: int = 4):
        self.rest_url = rest_url
        self.db_name = db_name
        self._session = requests.Session()
        self._session.auth = (user, password)
        self._session.headers.update({"Content-Type": "text/plain"})
        self._executor = ThreadPoolExecutor(max_workers=size)

    async def init(self):
        self._execute_sync(f"USE {self.db_name}")
        print(f"Connected to TDengine REST API at {self.rest_url}")

    def _execute_sync(self, sql: str):
        resp = self._session.post(self.rest_url, data=sql)
        resp.raise_for_status()
        result = resp.json()
        # if result.get("code") != 0:
        if result.get("status") != "succ":
            raise RuntimeError(f"TDengine error: {result.get('desc', result)}")
        return result

    async def execute(self, sql: str):
        """Execute SQL via REST API."""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._execute_sync, sql)

    async def close(self):
        self._session.close()
        self._executor.shutdown(wait=True)


# -------------------------
# Shared Buffer
# -------------------------
class SensorBuffer:
    """Thread-safe buffer for all sensors."""

    def __init__(self):
        self._data: dict[str, list[tuple[int, float]]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._total_rows = 0

    async def add(self, table: str, ts_ms: int, value: float):
        async with self._lock:
            self._data[table].append((ts_ms, value))
            self._total_rows += 1

    async def drain(self) -> dict[str, list[tuple[int, float]]]:
        """Return all data and reset buffer."""
        async with self._lock:
            data = dict(self._data)
            self._data = defaultdict(list)
            self._total_rows = 0
            return data

    @property
    def total_rows(self) -> int:
        return self._total_rows


# -------------------------
# Collector
# -------------------------
class OpcTDengineCollector:
    def __init__(self, sensors: list[tuple[str, str]]):
        self.sensors = sensors  # [(node_id, table_name), ...]
        self.buffer = SensorBuffer()
        self.pool: TDenginePool = None
        self._shutdown = asyncio.Event()
        self._subscriptions = []

    async def start(self):
        # Init connection pool
        # self.pool = TDenginePool(TD_CONNECTION, DB_NAME, DB_POOL_SIZE)
        self.pool = TDenginePool(TD_REST_URL, TD_USER, TD_PASSWORD, DB_NAME, DB_POOL_SIZE)
        await self.pool.init()

        # Start flush task
        flush_task = asyncio.create_task(self._flush_loop())

        # Connect to OPC UA and subscribe
        async with Client(url=OPC_UA_SERVER) as client:
            await self._setup_subscriptions(client)

            # Wait for shutdown signal
            await self._shutdown.wait()

            # Cleanup subscriptions
            for sub, handle in self._subscriptions:
                await sub.unsubscribe(handle)

        # Final flush
        flush_task.cancel()
        await self._flush_buffer()
        await self.pool.close()
        print("Shutdown complete.")

    async def _setup_subscriptions(self, client: Client):
        """Create one subscription per sensor."""

        for node_id, table_name in self.sensors:
            node = client.get_node(node_id)

            # Create handler closure capturing table_name
            buffer = self.buffer

            class Handler:
                def __init__(self, tbl):
                    self.table = tbl

                async def datachange_notification(self, node, val, data):
                    ts_ms = int(datetime.utcnow().timestamp() * 1000)
                    await buffer.add(self.table, ts_ms, float(val))

            sub = await client.create_subscription(500, Handler(table_name))
            handle = await sub.subscribe_data_change(node)
            self._subscriptions.append((sub, handle))
            print(f"Subscribed: {node_id} → {table_name}")

    async def _flush_loop(self):
        """Periodic flush task."""
        try:
            while True:
                await asyncio.sleep(FLUSH_INTERVAL_SEC)
                if self.buffer.total_rows >= FLUSH_ROW_THRESHOLD or self.buffer.total_rows > 0:
                    await self._flush_buffer()
        except asyncio.CancelledError:
            pass

    async def _flush_buffer(self):
        """Batch flush all sensors in one INSERT."""
        data = await self.buffer.drain()
        if not data:
            return

        # Build multi-table INSERT with auto-create:
        # INSERT INTO t1 USING sensors TAGS('t1') VALUES (...) (...) t2 USING sensors TAGS('t2') VALUES (...) ...
        parts = []
        total_rows = 0
        for table, rows in data.items():
            if rows:
                values = " ".join(f"({ts}, {val})" for ts, val in rows)
                parts.append(f"{DB_NAME}.{table} USING {DB_NAME}.sensors TAGS('{table}') VALUES {values}")
                total_rows += len(rows)

        if parts:
            sql = "INSERT INTO " + " ".join(parts)
            await self.pool.execute(sql)
            print(f"Flushed {total_rows} rows across {len(parts)} tables")

    def shutdown(self):
        self._shutdown.set()


# -------------------------
# Main
# -------------------------
async def main():
    collector = OpcTDengineCollector(SENSORS)

    # Handle Ctrl-C
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, collector.shutdown)

    await collector.start()


if __name__ == "__main__":
    asyncio.run(main())
