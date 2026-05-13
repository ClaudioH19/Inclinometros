import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from aiohttp import WSMsgType, web


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_TOPIC_ROOT = "inclinometro"
DEFAULT_KNOWN_NODES = ["Nodo_1", "Nodo_2", "Nodo_3"]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("clinostato")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_float(value):
    if value in (None, "", -999, "-999"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_int(value):
    numeric = parse_float(value)
    if numeric is None:
        return None
    return int(round(numeric))


def pick(payload: dict, keys):
    for key in keys:
        if key in payload and payload[key] not in (None, ""):
            return payload[key]
    return None


def extract_node_from_topic(topic_root: str, topic: str):
    parts = topic.split("/")
    if len(parts) >= 3 and parts[0] == topic_root:
        return parts[1]
    return None


def empty_node_state(node_id: str):
    return {
        "node_id": node_id,
        "connection_state": "unknown",
        "last_seen": None,
        "last_telemetry_at": None,
        "last_status_at": None,
        "last_command_at": None,
        "last_topic": None,
        "lux": None,
        "temperatura": None,
        "humedad": None,
        "presion": None,
        "co2": None,
        "tvoc": None,
        "rpm_m1": None,
        "rpm_m2": None,
        "target_rpm_m1": None,
        "target_rpm_m2": None,
        "last_payload_json": None,
    }


def node_to_api(node: dict, timeout_seconds: int):
    snapshot = dict(node)
    online = False
    if snapshot["last_seen"]:
        try:
            last_seen = datetime.fromisoformat(snapshot["last_seen"])
            delta = datetime.now(timezone.utc) - last_seen
            online = delta.total_seconds() <= timeout_seconds
        except ValueError:
            online = False

    snapshot["online"] = online
    return snapshot


class ClinostatoApp:
    def __init__(self):
        self.config = {
            "app_host": os.getenv("APP_HOST", "0.0.0.0"),
            "app_port": int(os.getenv("APP_PORT", "8000")),
            "mqtt_host": os.getenv("MQTT_HOST", "127.0.0.1"),
            "mqtt_port": int(os.getenv("MQTT_PORT", "1883")),
            "mqtt_topic_root": os.getenv("MQTT_TOPIC_ROOT", DEFAULT_TOPIC_ROOT).strip("/") or DEFAULT_TOPIC_ROOT,
            "db_path": os.getenv("DB_PATH", str(BASE_DIR / "data" / "clinostato.db")),
            "node_timeout_seconds": int(os.getenv("NODE_TIMEOUT_SECONDS", "30")),
            "known_nodes": self._load_known_nodes(),
        }

        db_path = Path(self.config["db_path"])
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init_db()

        self.nodes = {node_id: empty_node_state(node_id) for node_id in self.config["known_nodes"]}
        self.ws_clients = set()
        self.mqtt_events = asyncio.Queue()
        self.mqtt_consumer_task = None
        self.loop = None
        self.mqtt_client = None
        self._load_nodes_from_db()

    def _load_known_nodes(self):
        raw = os.getenv("KNOWN_NODES", ",".join(DEFAULT_KNOWN_NODES))
        nodes = [item.strip() for item in raw.split(",") if item.strip()]
        return nodes or list(DEFAULT_KNOWN_NODES)

    def _init_db(self):
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS node_state (
                node_id TEXT PRIMARY KEY,
                connection_state TEXT,
                last_seen TEXT,
                last_telemetry_at TEXT,
                last_status_at TEXT,
                last_command_at TEXT,
                last_topic TEXT,
                lux REAL,
                temperatura REAL,
                humedad REAL,
                presion REAL,
                co2 INTEGER,
                tvoc INTEGER,
                rpm_m1 REAL,
                rpm_m2 REAL,
                target_rpm_m1 REAL,
                target_rpm_m2 REAL,
                last_payload_json TEXT
            );

            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                received_at TEXT NOT NULL,
                lux REAL,
                temperatura REAL,
                humedad REAL,
                presion REAL,
                co2 INTEGER,
                tvoc INTEGER,
                rpm_m1 REAL,
                rpm_m2 REAL,
                payload_json TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                rpm_m1 REAL NOT NULL,
                rpm_m2 REAL NOT NULL,
                status TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            """
        )
        self.db.commit()

    def _load_nodes_from_db(self):
        rows = self.db.execute("SELECT * FROM node_state").fetchall()
        for row in rows:
            node_id = row["node_id"]
            self.nodes[node_id] = {
                **empty_node_state(node_id),
                **dict(row),
            }

    def build_topic(self, node_id: str, suffix: str):
        return f"{self.config['mqtt_topic_root']}/{node_id}/{suffix}"

    def readings_topic(self, node_id: str):
        return self.build_topic(node_id, "sensores")

    def status_topic(self, node_id: str):
        return self.build_topic(node_id, "estado")

    def command_topic(self, node_id: str):
        return self.build_topic(node_id, "motores/config")

    def start_mqtt(self):
        self.mqtt_client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="clinostato-server",
        )
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self.mqtt_client.on_message = self._on_mqtt_message
        self.mqtt_client.connect_async(
            self.config["mqtt_host"],
            self.config["mqtt_port"],
            keepalive=30,
        )
        self.mqtt_client.loop_start()

    def stop_mqtt(self):
        if not self.mqtt_client:
            return
        try:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        except Exception:
            log.exception("No se pudo detener MQTT")

    def _on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            sensors = f"{self.config['mqtt_topic_root']}/+/sensores"
            status = f"{self.config['mqtt_topic_root']}/+/estado"
            client.subscribe(sensors)
            client.subscribe(status)
            log.info("MQTT conectado a %s:%s", self.config["mqtt_host"], self.config["mqtt_port"])
            log.info("Suscrito a %s y %s", sensors, status)
            return

        log.error("Fallo de conexion MQTT: %s", reason_code)

    def _on_mqtt_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        log.warning("MQTT desconectado: %s", reason_code)

    def _on_mqtt_message(self, client, userdata, message):
        raw = message.payload.decode("utf-8", errors="ignore").strip()
        if not raw:
            return

        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(
                self.mqtt_events.put_nowait,
                {
                    "topic": message.topic,
                    "raw": raw,
                    "received_at": now_iso(),
                },
            )

    async def consume_mqtt_events(self):
        while True:
            event = await self.mqtt_events.get()
            try:
                await self.process_mqtt_event(event)
            except Exception:
                log.exception("Error procesando evento MQTT")

    async def process_mqtt_event(self, event: dict):
        topic = event["topic"]
        received_at = event["received_at"]
        raw = event["raw"]

        try:
            payload = json.loads(raw)
            if not isinstance(payload, dict):
                raise ValueError("JSON no es objeto")
        except Exception:
            payload = {"raw": raw}

        node_id = payload.get("placa_id") or extract_node_from_topic(self.config["mqtt_topic_root"], topic)
        if not node_id:
            log.warning("Se recibio mensaje MQTT sin nodo: %s", topic)
            return

        payload["placa_id"] = node_id
        node = self.nodes.get(node_id, empty_node_state(node_id))
        node["node_id"] = node_id
        node["last_seen"] = received_at
        node["last_topic"] = topic
        node["last_payload_json"] = json.dumps(payload, ensure_ascii=False)

        if topic.endswith("/sensores"):
            node["connection_state"] = "online"
            node["last_telemetry_at"] = received_at
            node["lux"] = parse_float(pick(payload, ["lux", "light", "luz"]))
            node["temperatura"] = parse_float(pick(payload, ["temperatura", "temp", "temperature"]))
            node["humedad"] = parse_float(pick(payload, ["humedad", "hum", "humidity"]))
            node["presion"] = parse_float(pick(payload, ["presion", "pres", "pressure"]))
            node["co2"] = parse_int(pick(payload, ["co2", "eco2", "eCO2"]))
            node["tvoc"] = parse_int(pick(payload, ["tvoc", "TVOC"]))
            node["rpm_m1"] = parse_float(pick(payload, ["rpm_m1", "rpm1", "rpmMotor1"]))
            node["rpm_m2"] = parse_float(pick(payload, ["rpm_m2", "rpm2", "rpmMotor2"]))
            self._insert_sensor_reading(node_id, topic, received_at, node, payload)

        elif topic.endswith("/estado"):
            node["last_status_at"] = received_at
            node["connection_state"] = str(payload.get("estado") or "online")
            maybe_rpm_1 = parse_float(pick(payload, ["rpm_m1", "rpm1", "rpmMotor1"]))
            maybe_rpm_2 = parse_float(pick(payload, ["rpm_m2", "rpm2", "rpmMotor2"]))
            if maybe_rpm_1 is not None:
                node["rpm_m1"] = maybe_rpm_1
            if maybe_rpm_2 is not None:
                node["rpm_m2"] = maybe_rpm_2
        else:
            log.info("Mensaje MQTT ignorado: %s", topic)
            return

        self.nodes[node_id] = node
        self._upsert_node_state(node)

        await self.broadcast(
            {
                "type": "node_update",
                "node": node_to_api(node, self.config["node_timeout_seconds"]),
                "received_at": received_at,
            }
        )

    def _insert_sensor_reading(self, node_id: str, topic: str, received_at: str, node: dict, payload: dict):
        self.db.execute(
            """
            INSERT INTO sensor_readings (
                node_id, topic, received_at, lux, temperatura, humedad, presion,
                co2, tvoc, rpm_m1, rpm_m2, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                topic,
                received_at,
                node["lux"],
                node["temperatura"],
                node["humedad"],
                node["presion"],
                node["co2"],
                node["tvoc"],
                node["rpm_m1"],
                node["rpm_m2"],
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        self.db.commit()

    def _upsert_node_state(self, node: dict):
        self.db.execute(
            """
            INSERT INTO node_state (
                node_id, connection_state, last_seen, last_telemetry_at, last_status_at,
                last_command_at, last_topic, lux, temperatura, humedad, presion,
                co2, tvoc, rpm_m1, rpm_m2, target_rpm_m1, target_rpm_m2, last_payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(node_id) DO UPDATE SET
                connection_state = excluded.connection_state,
                last_seen = excluded.last_seen,
                last_telemetry_at = excluded.last_telemetry_at,
                last_status_at = excluded.last_status_at,
                last_command_at = excluded.last_command_at,
                last_topic = excluded.last_topic,
                lux = excluded.lux,
                temperatura = excluded.temperatura,
                humedad = excluded.humedad,
                presion = excluded.presion,
                co2 = excluded.co2,
                tvoc = excluded.tvoc,
                rpm_m1 = excluded.rpm_m1,
                rpm_m2 = excluded.rpm_m2,
                target_rpm_m1 = excluded.target_rpm_m1,
                target_rpm_m2 = excluded.target_rpm_m2,
                last_payload_json = excluded.last_payload_json
            """,
            (
                node["node_id"],
                node["connection_state"],
                node["last_seen"],
                node["last_telemetry_at"],
                node["last_status_at"],
                node["last_command_at"],
                node["last_topic"],
                node["lux"],
                node["temperatura"],
                node["humedad"],
                node["presion"],
                node["co2"],
                node["tvoc"],
                node["rpm_m1"],
                node["rpm_m2"],
                node["target_rpm_m1"],
                node["target_rpm_m2"],
                node["last_payload_json"],
            ),
        )
        self.db.commit()

    def _insert_command(self, node_id: str, topic: str, requested_at: str, rpm_m1: float, rpm_m2: float, status: str):
        payload = {
            "placa_id": node_id,
            "rpm_m1": rpm_m1,
            "rpm_m2": rpm_m2,
            "requested_at": requested_at,
            "status": status,
        }
        self.db.execute(
            """
            INSERT INTO commands (node_id, topic, requested_at, rpm_m1, rpm_m2, status, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                node_id,
                topic,
                requested_at,
                rpm_m1,
                rpm_m2,
                status,
                json.dumps(payload, ensure_ascii=False),
            ),
        )
        self.db.commit()

    async def publish_rpm_command(self, node_id: str, rpm_m1: float, rpm_m2: float):
        if not self.mqtt_client or not self.mqtt_client.is_connected():
            raise RuntimeError("El broker MQTT no esta conectado")

        requested_at = now_iso()
        topic = self.command_topic(node_id)
        payload = {
            "placa_id": node_id,
            "rpm_m1": rpm_m1,
            "rpm_m2": rpm_m2,
            "server_ts": requested_at,
        }

        info = self.mqtt_client.publish(topic, json.dumps(payload), qos=1, retain=False)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise RuntimeError(f"No se pudo publicar comando MQTT: rc={info.rc}")

        node = self.nodes.get(node_id, empty_node_state(node_id))
        node["node_id"] = node_id
        node["target_rpm_m1"] = rpm_m1
        node["target_rpm_m2"] = rpm_m2
        node["last_command_at"] = requested_at
        self.nodes[node_id] = node
        self._upsert_node_state(node)
        self._insert_command(node_id, topic, requested_at, rpm_m1, rpm_m2, "published")

        await self.broadcast(
            {
                "type": "command_ack",
                "node_id": node_id,
                "rpm_m1": rpm_m1,
                "rpm_m2": rpm_m2,
                "topic": topic,
                "requested_at": requested_at,
            }
        )

    def get_nodes_snapshot(self):
        ordered_ids = []
        for node_id in self.config["known_nodes"]:
            ordered_ids.append(node_id)
        for node_id in sorted(self.nodes.keys()):
            if node_id not in ordered_ids:
                ordered_ids.append(node_id)

        return [
            node_to_api(self.nodes.get(node_id, empty_node_state(node_id)), self.config["node_timeout_seconds"])
            for node_id in ordered_ids
        ]

    def get_recent_commands(self, limit: int):
        rows = self.db.execute(
            """
            SELECT node_id, topic, requested_at, rpm_m1, rpm_m2, status
            FROM commands
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    async def broadcast(self, payload: dict):
        dead = []
        for ws in list(self.ws_clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.ws_clients.discard(ws)


state = ClinostatoApp()


async def index(request):
    return web.FileResponse(BASE_DIR / "index.html")


async def api_health(request):
    return web.json_response(
        {
            "status": "ok",
            "mqtt_connected": bool(state.mqtt_client and state.mqtt_client.is_connected()),
            "server_ts": now_iso(),
        }
    )


async def api_nodes(request):
    return web.json_response(
        {
            "nodes": state.get_nodes_snapshot(),
            "server_ts": now_iso(),
        }
    )


async def api_commands(request):
    limit = request.query.get("limit", "20")
    try:
        limit = max(1, min(100, int(limit)))
    except ValueError:
        limit = 20

    return web.json_response(
        {
            "commands": state.get_recent_commands(limit),
            "server_ts": now_iso(),
        }
    )


async def api_set_rpm(request):
    node_id = request.match_info["node_id"]
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "JSON invalido"}, status=400)

    rpm_m1 = parse_float(body.get("rpm_m1"))
    rpm_m2 = parse_float(body.get("rpm_m2"))

    if rpm_m1 is None or rpm_m2 is None:
        return web.json_response({"error": "rpm_m1 y rpm_m2 son obligatorios"}, status=400)

    if rpm_m1 < 0 or rpm_m2 < 0:
        return web.json_response({"error": "Las RPM no pueden ser negativas"}, status=400)

    try:
        await state.publish_rpm_command(node_id, rpm_m1, rpm_m2)
    except RuntimeError as exc:
        return web.json_response({"error": str(exc)}, status=503)

    return web.json_response(
        {
            "ok": True,
            "node_id": node_id,
            "rpm_m1": rpm_m1,
            "rpm_m2": rpm_m2,
            "server_ts": now_iso(),
        }
    )


async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    state.ws_clients.add(ws)

    await ws.send_json(
        {
            "type": "snapshot",
            "nodes": state.get_nodes_snapshot(),
            "commands": state.get_recent_commands(20),
            "server_ts": now_iso(),
        }
    )

    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            text = msg.data.strip()
            if text == "ping":
                await ws.send_str("pong")
        elif msg.type == WSMsgType.ERROR:
            log.warning("WebSocket error: %s", ws.exception())

    state.ws_clients.discard(ws)
    return ws


async def startup(app):
    state.loop = asyncio.get_running_loop()
    state.start_mqtt()
    state.mqtt_consumer_task = asyncio.create_task(state.consume_mqtt_events())
    log.info("HTTP disponible en http://%s:%s", state.config["app_host"], state.config["app_port"])


async def cleanup(app):
    if state.mqtt_consumer_task:
        state.mqtt_consumer_task.cancel()
        try:
            await state.mqtt_consumer_task
        except asyncio.CancelledError:
            pass
    state.stop_mqtt()
    for ws in list(state.ws_clients):
        await ws.close()
    state.db.close()


def build_app():
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/api/health", api_health)
    app.router.add_get("/api/nodes", api_nodes)
    app.router.add_get("/api/commands", api_commands)
    app.router.add_post("/api/nodes/{node_id}/rpm", api_set_rpm)
    app.on_startup.append(startup)
    app.on_cleanup.append(cleanup)
    return app


if __name__ == "__main__":
    web.run_app(
        build_app(),
        host=state.config["app_host"],
        port=state.config["app_port"],
    )
