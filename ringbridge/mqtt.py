"""
MQTT output for ringbridge.

The Ring push carries more than fits into the stream: a description
sentence written by Ring's LLM, the classification (human/other_motion)
and a snapshot URL. None of that is available through the history API,
where `short_description`/`full_description` are consistently `null`.

This publishes those details to MQTT so Home Assistant can show them
alongside the Frigate cameras. Deliberately **not** under `frigate/...` -
that namespace belongs to Frigate. Own prefix, plus optional Home
Assistant discovery so the entities appear by themselves.

If MQTT is unavailable, the rest of ringbridge carries on unchanged: every
error here is caught and only logged.
"""

import json
import logging
import threading

from ringbridge.config import *


log = logging.getLogger(__name__)

# Fields published per camera.
FIELDS = ('description', 'title', 'detection', 'snapshot_url', 'timestamp')

# Only these get a Home Assistant discovery entity; snapshot_url and
# timestamp are attributes, not sensors of their own.
#
# The label is only the friendly name. The entity's identity comes from
# unique_id, which is built from the FIELD name below, so renaming a label
# updates the display name and keeps the entity - existing automations are
# unaffected. Note that Home Assistant does not rename an entity_id that
# was already derived from an older label; only a fresh entity picks up the
# new wording.
DISCOVERY_FIELDS = {
    'description': ('Description', 'mdi:text-short'),
    'detection':   ('Detection',   'mdi:motion-sensor'),
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
            log.info("MQTT disabled (mqtt.enabled = false)")
            return

        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            log.error("paho-mqtt not installed - continuing without MQTT")
            self.enabled = False
            return

        try:
            client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=self._cfg.get('client_id', 'ringbridge'))

            user = self._cfg.get('username')
            if user:
                client.username_pw_set(user, self._cfg.get('password'))

            # Last will, so HA can see when the bridge goes away.
            availability = f"{self.prefix}/status"
            client.will_set(availability, "offline", retain=True)

            client.connect(self._cfg.get('host', '127.0.0.1'),
                           int(self._cfg.get('port', 1883)),
                           keepalive=60)
            client.loop_start()
            client.publish(availability, "online", retain=True)

            self.client = client
            log.info(f"MQTT connected to {self._cfg.get('host')}:"
                     f"{self._cfg.get('port', 1883)}, prefix '{self.prefix}'")
        except Exception as e:
            log.error(f"MQTT connection failed ({e}) - continuing without MQTT")
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
            log.debug(f"MQTT stop: {e}")
        self.client = None

    # ------------------------------------------------------------------

    def _announce(self, camera_key: str, camera_name: str) -> None:
        """Home Assistant discovery per camera - once per run."""
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
        log.info(f"MQTT: discovery published for {camera_name}")

    def publish_event(self, camera_key: str, camera_name: str, values: dict) -> None:
        """
        Publish one Ring push event.

        `camera_key` is the sanitised name (as used in the RTSP path), so
        the topics line up with the Frigate camera names.
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

            log.debug(f"MQTT: published {camera_name}")
        except Exception as e:
            log.error(f"MQTT publish failed: {e}")
