import datetime as dt
import json
import logging
import ssl
import threading
from datetime import datetime
from typing import Any, Callable, Dict, Optional

try:
    import paho.mqtt.client as mqtt

    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False

from .storage_utils import PacketRecord
from .. import __version__

logger = logging.getLogger("MQTTHandler")

UTC = getattr(dt, "UTC", dt.timezone.utc)


class MQTTHandler:
    def __init__(
        self,
        mqtt_config: dict,
        node_name: str = "unknown",
        node_id: str = "unknown",
        *,
        auto_connect: bool = True,
        full_config: Optional[dict] = None,
        stats_provider: Optional[Callable[[], dict]] = None,
    ):
        self.mqtt_config = mqtt_config
        self.node_name = node_name
        self.node_id = node_id
        self.client = None
        self.available = MQTT_AVAILABLE
        self.auto_connect = auto_connect
        self._connected = False
        self.full_config = full_config or {}
        self.stats_provider = stats_provider
        self.schema = self.mqtt_config.get("schema", "default")
        self._status_task = None
        self._status_running = False

        self._letsmesh_iata_code = None
        self._letsmesh_status_interval = 0
        self._letsmesh_radio_config = None
        if self.schema == "letsmesh":
            from ..config import get_node_info

            node_info = get_node_info(self.full_config)
            self._letsmesh_iata_code = node_info["iata_code"]
            self._letsmesh_status_interval = node_info["status_interval"]
            self._letsmesh_radio_config = node_info["radio_config"]

        broker = self.mqtt_config.get("broker", "localhost")
        port = self.mqtt_config.get("port", 1883)
        self.broker = {
            "name": self.mqtt_config.get("name", broker),
            "host": broker,
            "port": port,
        }

        self._init_client()

    def _init_client(self):
        if not self.available or not self.mqtt_config.get("enabled", False):
            logger.info("MQTT disabled or not available")
            return
            
        try:
            # Use WebSocket transport if configured, otherwise use standard TCP
            transport = "websockets" if self.mqtt_config.get("use_websockets", False) else "tcp"
            client_id = self.mqtt_config.get("client_id")
            self.client = mqtt.Client(client_id=client_id, transport=transport)
            self.client.on_connect = self._handle_connect
            self.client.on_disconnect = self._handle_disconnect
            
            if transport == "websockets":
                logger.info("Using WebSocket transport for MQTT")
            
            # Configure TLS/SSL if enabled
            tls_config = self.mqtt_config.get("tls", {})
            if tls_config.get("enabled", False):
                tls_params = {
                    "cert_reqs": ssl.CERT_REQUIRED,
                    "tls_version": ssl.PROTOCOL_TLS_CLIENT,
                }
                
                # CA certificate for server verification (optional - uses system certs if not specified)
                ca_cert = tls_config.get("ca_cert")
                if ca_cert:
                    tls_params["ca_certs"] = ca_cert
                    logger.info("Using custom CA certificate for MQTT TLS")
                else:
                    logger.info("Using system default CA certificates for MQTT TLS")
                
                # Client certificate and key (for mutual TLS)
                client_cert = tls_config.get("client_cert")
                client_key = tls_config.get("client_key")
                if client_cert:
                    tls_params["certfile"] = client_cert
                if client_key:
                    tls_params["keyfile"] = client_key
                
                # Allow insecure connections (skip cert verification)
                if tls_config.get("insecure", False):
                    tls_params["cert_reqs"] = ssl.CERT_NONE
                    logger.warning("MQTT TLS certificate verification disabled (insecure mode)")
                
                self.client.tls_set(**tls_params)
                self.client.tls_insecure_set(bool(tls_config.get("insecure", False)))
                logger.info("MQTT TLS/SSL configured")
            
            username = self.mqtt_config.get("username")
            password = self.mqtt_config.get("password")
            if username:
                self.client.username_pw_set(username, password)
            
            if self.auto_connect:
                self.connect()
            
        except Exception as e:
            logger.error(f"Failed to initialize MQTT: {e}")
            self.client = None
            self._connected = False

    def _handle_connect(self, client, userdata, flags, rc):
        self._connected = rc == 0
        if rc == 0:
            logger.info(f"MQTT client connected to {self.broker['host']}:{self.broker['port']}")
            if self.schema == "letsmesh":
                self._start_letsmesh_status_loop()
        else:
            logger.error(
                f"MQTT connection failed to {self.broker['host']}:{self.broker['port']} with code {rc}"
            )

    def _handle_disconnect(self, client, userdata, rc):
        self._connected = False
        self._status_running = False
        self._status_task = None
        if rc == 0:
            logger.info(
                f"MQTT client disconnected from {self.broker['host']}:{self.broker['port']}"
            )
        else:
            logger.warning(
                f"MQTT client disconnected unexpectedly from {self.broker['host']}:{self.broker['port']} (rc={rc})"
            )

    def connect(self):
        if not self.client:
            return False

        try:
            tls_config = self.mqtt_config.get("tls", {})
            secure = "(TLS)" if tls_config.get("enabled", False) else ""
            logger.info(
                f"Connecting to MQTT broker {self.broker['host']}:{self.broker['port']} {secure}..."
            )
            self.client.connect(self.broker["host"], self.broker["port"], 60)
            self.client.loop_start()
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self._connected = False
            return False

    def _start_letsmesh_status_loop(self):
        if self._letsmesh_status_interval <= 0:
            return

        if self._status_task and self._status_task.is_alive():
            return

        self._status_running = True
        self.publish_status(state="online")
        self._status_task = threading.Thread(target=self._status_heartbeat_loop, daemon=True)
        self._status_task.start()

    def _status_heartbeat_loop(self):
        import time

        while self._status_running:
            try:
                self.publish_status(state="online")
                time.sleep(self._letsmesh_status_interval)
            except Exception as e:
                logger.error(f"MQTT LetsMesh status heartbeat error: {e}")
                time.sleep(self._letsmesh_status_interval)

    def publish_message(self, topic: str, payload: Any, qos: int = 0, retain: bool = False):
        if not self.client:
            return None

        try:
            message = payload if isinstance(payload, str) else json.dumps(payload, default=str)
            result = self.client.publish(topic, message, qos=qos, retain=retain)
            logger.debug(f"Published to {topic}")
            return result
        except Exception as e:
            logger.error(f"Failed to publish message to MQTT: {e}")
            return None

    def publish(self, record: dict, record_type: str):
        """
        Publish record to MQTT.
        Packets MUST use PacketRecord format. Non-packet records use original format.
        
        Args:
            record: The record dictionary to publish
            record_type: Type of record (packet, advert, noise_floor, etc.)
        """
        if not self.client:
            return
            
        try:
            topic = self._topic_for_record_type(record_type)

            if self.schema == "letsmesh" and record_type == "packet":
                packet_record = PacketRecord.from_packet_record(
                    record, origin=self.node_name, origin_id=self.node_id
                )
                if not packet_record:
                    logger.debug(
                        "Skipping MQTT publish: packet missing required data for PacketRecord"
                    )
                    return

                payload = self._process_letsmesh_packet(packet_record.to_dict())
                logger.debug("Publishing packet using LetsMesh MQTT schema")
            elif record_type == "packet":
                packet_record = PacketRecord.from_packet_record(
                    record, origin=self.node_name, origin_id=self.node_id
                )
                if not packet_record:
                    logger.debug(
                        "Skipping MQTT publish: packet missing required data for PacketRecord"
                    )
                    return

                payload = packet_record.to_dict()
                logger.debug("Publishing packet using PacketRecord format")
            else:
                payload = {k: v for k, v in record.items() if v is not None}
            
            self.publish_message(topic, payload, qos=0, retain=False)
            
        except Exception as e:
            logger.error(f"Failed to publish to MQTT: {e}")

    def _process_letsmesh_packet(self, pkt: dict) -> dict:
        return {"timestamp": datetime.now(UTC).isoformat(), "origin_id": self.node_id, **pkt}

    def _topic_for_record_type(self, record_type: str) -> str:
        if self.schema == "letsmesh" and record_type == "packet":
            return f"meshcore/{self._letsmesh_iata_code}/{self.node_id}/packets"

        base_topic = self.mqtt_config.get("base_topic", "meshcore/repeater")
        return f"{base_topic}/{self.node_name}/{record_type}"

    def publish_status(
        self,
        state: str = "online",
        location: Optional[dict] = None,
        extra_stats: Optional[dict] = None,
    ):
        if self.schema != "letsmesh" or not self.client:
            return None

        if self.stats_provider:
            live_stats = self.stats_provider()
        else:
            live_stats = {"uptime_secs": 0, "packets_sent": 0, "packets_received": 0}

        status = {
            "status": state,
            "timestamp": datetime.now(UTC).isoformat(),
            "origin": self.node_name,
            "origin_id": self.node_id,
            "model": "PyMC-Repeater",
            "firmware_version": __version__,
            "radio": self._letsmesh_radio_config,
            "client_version": f"pyMC_repeater/{__version__}",
            "stats": {**live_stats, "errors": 0, "queue_len": 0, **(extra_stats or {})},
        }

        if location:
            status["location"] = location

        topic = f"meshcore/{self._letsmesh_iata_code}/{self.node_id}/status"
        return self.publish_message(topic, status, qos=0, retain=False)

    def close(self):
        if self.client:
            if self.schema == "letsmesh" and self._connected:
                self.publish_status(state="offline")
            self._status_running = False
            self._status_task = None
            self._connected = False
            self.client.loop_stop()
            self.client.disconnect()
            logger.info("MQTT client disconnected")