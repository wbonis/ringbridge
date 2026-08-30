"""
MQTT-Ausgabe fuer ringbridge.

Ring liefert im Push mehr, als in den Stream passt: einen von Ring's LLM
geschriebenen Beschreibungssatz, die Klassifikation (human/other_motion)
und eine Snapshot-URL. In der History-API fehlt all das (dort sind
`short_description`/`full_description` durchgaengig `null`).

Das hier veroeffentlicht diese Angaben nach MQTT, damit Home Assistant
sie neben den Frigate-Kameras anzeigen kann. Bewusst **nicht** unter
`frigate/...` - das gehoert Frigate. Eigener Praefix, plus optionale
HA-Discovery, damit die Entitaeten von selbst entstehen.

Faellt MQTT aus, laeuft der Rest von ringbridge unveraendert weiter:
alle Fehler werden geschluckt und nur geloggt.
"""

import json
import logging
import threading

from ringbridge.config import *


log = logging.getLogger(__name__)

# Felder, die je Kamera veroeffentlicht werden.
FIELDS = ('description', 'title', 'detection', 'snapshot_url', 'timestamp')

# Nur diese bekommen eine HA-Discovery-Entitaet; snapshot_url und
# timestamp sind Attribute, keine eigenen Sensoren.
DISCOVERY_FIELDS = {
    'description': ('Beschreibung', 'mdi:text-short'),
    'detection':   ('Erkennung',    'mdi:motion-sensor'),
}


class MqttPublisher:
    def __init__(self):
        self.client = None
        self._announced = set()
        self._lock = threading.Lock()

        cfg = CONFIG.get('mqtt') or {}
        self.enabled = bool(cfg.get('enabled'))
        self.prefix = cfg.get('topic_prefix', 'ringbridge')
        self.discovery = bool(cfg.get('discovery', True))
        self.discovery_prefix = cfg.get('discovery_prefix', 'homeassistant')
        self._cfg = cfg

    def start(self) -> None:
        if not self.enabled:
            log.info("MQTT deaktiviert (mqtt.enabled = false)")
            return

        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            log.error("paho-mqtt nicht installiert - MQTT bleibt aus")
            self.enabled = False
            return

        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=self._cfg.get('client_id', 'ringbridge'))

            user = self._cfg.get('username')
            if user:
                client.username_pw_set(user, self._cfg.get('password'))

            # Letzter Wille: HA sieht, wenn die Bruecke weg ist.
            availability = f"{self.prefix}/status"
            client.will_set(availability, "offline", retain=True)

            client.connect(self._cfg.get('host', '127.0.0.1'),
                           int(self._cfg.get('port', 1883)),
                           keepalive=60)
            client.loop_start()
            client.publish(availability, "online", retain=True)

            self.client = client
            log.info(f"MQTT verbunden mit {self._cfg.get('host')}:"
                     f"{self._cfg.get('port', 1883)}, Praefix '{self.prefix}'")
        except Exception as e:
            log.error(f"MQTT-Verbindung fehlgeschlagen ({e}) - laeuft ohne MQTT weiter")
            self.client = None
            self.enabled = False

    def stop(self) -> None:
        if self.client is None:
            return
        try:
            self.client.publish(f"{self.prefix}/status", "offline", retain=True)
            self.client.loop_stop()
            self.client.disconnect()
        except Exception as e:
            log.debug(f"MQTT-Stop: {e}")
        self.client = None

    # ------------------------------------------------------------------

    def _announce(self, camera_key: str, camera_name: str) -> None:
        """HA-Discovery je Kamera - einmal pro Laufzeit."""
        if not self.discovery or camera_key in self._announced:
            return

        device = {
            "identifiers": [f"ringbridge_{camera_key}"],
            "name": f"Ring {camera_name}",
            "manufacturer": "Ring",
            "model": "via ringbridge",
        }

        for field, (label, icon) in DISCOVERY_FIELDS.items():
            topic = (f"{self.discovery_prefix}/sensor/"
                     f"ringbridge_{camera_key}_{field}/config")
            payload = {
                "name": label,
                "unique_id": f"ringbridge_{camera_key}_{field}",
                "state_topic": f"{self.prefix}/{camera_key}/{field}",
                "json_attributes_topic": f"{self.prefix}/{camera_key}/attributes",
                "availability_topic": f"{self.prefix}/status",
                "icon": icon,
                "device": device,
            }
            self.client.publish(topic, json.dumps(payload), retain=True)

        self._announced.add(camera_key)
        log.info(f"MQTT: Discovery fuer {camera_name} veroeffentlicht")

    def publish_event(self, camera_key: str, camera_name: str, values: dict) -> None:
        """
        Ein Ring-Push-Ereignis veroeffentlichen.

        `camera_key` ist der bereinigte Name (wie im RTSP-Pfad), damit die
        Themen zu den Frigate-Kameranamen passen.
        """
        if self.client is None:
            return

        try:
            with self._lock:
                self._announce(camera_key, camera_name)

                for field in FIELDS:
                    value = values.get(field)
                    if value is None:
                        continue
                    self.client.publish(f"{self.prefix}/{camera_key}/{field}",
                                        str(value), retain=True)

                self.client.publish(f"{self.prefix}/{camera_key}/attributes",
                                    json.dumps(values, ensure_ascii=False),
                                    retain=True)

            log.debug(f"MQTT: {camera_name} veroeffentlicht")
        except Exception as e:
            log.error(f"MQTT-Veroeffentlichung fehlgeschlagen: {e}")
