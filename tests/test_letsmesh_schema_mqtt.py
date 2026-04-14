from unittest.mock import MagicMock, patch

from repeater.config import get_node_info
from repeater.data_acquisition.letsmesh_handler import MeshCoreToMqttJwtPusher
from repeater.data_acquisition.mqtt_handler import MQTTHandler


class FakePublicKey:
    def __init__(self, value: str):
        self._value = value

    def hex(self) -> str:
        return self._value


class FakeIdentity:
    def __init__(self, public_key: str):
        self._public_key = FakePublicKey(public_key)

    def get_public_key(self):
        return self._public_key


class FakeTransport:
    def __init__(self):
        self.broker = {"name": "Test Broker", "host": "mqtt.example.com", "port": 1883}
        self.connected = True
        self.published = []
        self.connect_calls = 0
        self.close_calls = 0

    def connect(self):
        self.connect_calls += 1
        self.connected = True
        return True

    def close(self):
        self.close_calls += 1
        self.connected = False

    def is_connected(self) -> bool:
        return self.connected

    def has_pending_reconnect(self) -> bool:
        return False

    def publish_message(self, topic, payload, qos=0, retain=False):
        self.published.append(
            {
                "topic": topic,
                "payload": payload,
                "qos": qos,
                "retain": retain,
            }
        )
        return True


def make_config():
    return {
        "repeater": {"node_name": "test-repeater"},
        "radio": {
            "frequency": 869618000,
            "bandwidth": 62500,
            "spreading_factor": 8,
            "coding_rate": 8,
        },
        "letsmesh": {
            "enabled": True,
            "iata_code": "LHR",
            "broker_index": 0,
            "status_interval": 300,
            "email": "",
            "owner": "",
            "disallowed_packet_types": [],
        },
        "mqtt": {
            "enabled": True,
            "broker": "mqtt.example.com",
            "port": 1883,
            "schema": "letsmesh",
        },
    }


def test_get_node_info_keeps_broker_index():
    node_info = get_node_info(make_config())

    assert node_info["broker_index"] == 0
    assert node_info["iata_code"] == "LHR"


def test_plain_mqtt_letsmesh_schema_publishes_packet_to_meshcore_topic():
    mqtt_config = make_config()["mqtt"]

    with patch("repeater.data_acquisition.mqtt_handler.mqtt.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        handler = MQTTHandler(
            mqtt_config,
            node_name="test-repeater",
            node_id="A1B2C3",
            auto_connect=False,
            full_config=make_config(),
        )
        handler.publish(
            {
                "timestamp": 1710000000,
                "raw_packet": "AABB",
                "type": 4,
                "payload_length": 2,
                "route": 1,
                "snr": 7.5,
                "rssi": -80,
                "score": 0.9,
                "packet_hash": "deadbeef",
            },
            "packet",
        )

        args, kwargs = mock_client.publish.call_args
        assert args[0] == "meshcore/LHR/A1B2C3/packets"
        assert '"origin_id": "A1B2C3"' in args[1]
        assert '"raw": "AABB"' in args[1]
        assert kwargs["retain"] is False


def test_plain_mqtt_letsmesh_schema_publishes_status_to_meshcore_topic():
    mqtt_config = make_config()["mqtt"]

    with patch("repeater.data_acquisition.mqtt_handler.mqtt.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        handler = MQTTHandler(
            mqtt_config,
            node_name="test-repeater",
            node_id="DEADBEEF",
            auto_connect=False,
            full_config=make_config(),
            stats_provider=lambda: {"uptime_secs": 1, "packets_sent": 2, "packets_received": 3},
        )
        handler.publish_status(state="online")

        args, kwargs = mock_client.publish.call_args
        assert args[0] == "meshcore/LHR/DEADBEEF/status"
        assert '"status": "online"' in args[1]
        assert '"origin_id": "DEADBEEF"' in args[1]
        assert '"client_version": "pyMC_repeater/' in args[1]
        assert kwargs["retain"] is False


def test_mqtt_handler_publish_message_serializes_payload():
    mqtt_config = {"enabled": True, "broker": "mqtt.example.com", "port": 1883}

    with patch("repeater.data_acquisition.mqtt_handler.mqtt.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        handler = MQTTHandler(mqtt_config, auto_connect=False)
        handler.publish_message("meshcore/test/topic", {"hello": "world"}, retain=True)

        mock_client.publish.assert_called_once()
        args, kwargs = mock_client.publish.call_args
        assert args[0] == "meshcore/test/topic"
        assert '"hello": "world"' in args[1]
        assert kwargs["retain"] is True


def test_letsmesh_handler_still_initializes_connections():
    with patch("repeater.data_acquisition.letsmesh_handler._BrokerConnection") as mock_conn_cls:
        mock_conn_cls.return_value = MagicMock()

        handler = MeshCoreToMqttJwtPusher(
            local_identity=FakeIdentity("deadbeef"),
            config=make_config(),
        )

        assert len(handler.connections) == 1
