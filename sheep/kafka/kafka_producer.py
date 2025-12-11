from __future__ import annotations

import logging
import time
import uuid
from queue import Queue, Empty
from typing import Any, Tuple, Optional

from .kafka_config import build_kafka_producer, load_kafka_config


class KafkaDataProducer:
    
    
    
    def __init__(self, buy_signal_queue: Queue, cfg: dict, log: logging.Logger):

        producer_kwargs = load_kafka_config(cfg["kafka_properties_path"])
        self.producer = build_kafka_producer(producer_kwargs)
        self.action_topic = cfg["action_topic"]
        self.heartbeat_topic = cfg["heartbeat_topic"]
        self.buy_signal_queue = buy_signal_queue

        # seconds
        self.sleep_time = float(cfg.get("sleep_time", 600.0))       # 10 mins = 600 sec
        self.heart_beat_freq = float(cfg.get("heart_beat_freq", 3600.0))  # 1 hour = 3600 sec

        # optional heartbeat topic
        self.heartbeat_topic: Optional[str] = cfg.get("heartbeat_topic", None)

        # stats
        self.total_sent = 0
        self.sent_since_heartbeat = 0

        self.log = log



    def send_message(self, topic: str, message: Any) -> None:
        """Send a single Kafka message."""
        key_str = str(uuid.uuid4())
        key_bytes = key_str.encode("utf-8")
        self.producer.send(topic, key=key_bytes, value=message)



    def _send_heartbeat(self, qsize: int) -> None:
        """Send a heartbeat message with stats since last heartbeat."""
        if not self.heartbeat_topic:
            return  # logging-only heartbeat if no topic configured

        payload = {
            "type": "sheep_heartbeat",
            "timestamp": time.time(),
            "queue_size": qsize,
            "total_sent": self.total_sent,
            "sent_since_heartbeat": self.sent_since_heartbeat,
        }

        try:
            self.send_message(self.heartbeat_topic, payload)
            self.log.info(
                "Heartbeat sent: topic=%s qsize=%s total_sent=%s sent_since_hb=%s",
                self.heartbeat_topic,
                qsize,
                self.total_sent,
                self.sent_since_heartbeat,
            )
        except Exception:
            self.log.exception("Failed to send heartbeat message")

        # reset interval counter
        self.sent_since_heartbeat = 0



    def producer_loop(self, poll_timeout_s: float = 0.5, batch_window_s: float = 5.0) -> None:
        self.log.info("Kafka producer loop started")
        last_heartbeat = time.monotonic()

        try:
            while True:
                now = time.monotonic()

                # Heartbeat (log + optional Kafka message)
                if self.heart_beat_freq > 0 and (now - last_heartbeat) >= self.heart_beat_freq:
                    try:
                        qsize = self.buy_signal_queue.qsize()
                    except Exception:
                        qsize = -1

                    self.log.info(
                        "KafkaDataProducer heartbeat: queue_size=%s topic=%s",
                        qsize,
                        self.default_topic,
                    )
                    self._send_heartbeat(qsize)
                    last_heartbeat = now

                # Optional throttle
                if self.sleep_time > 0:
                    time.sleep(self.sleep_time)

                # Read one item from the queue
                try:
                    item = self.buy_signal_queue.get(timeout=poll_timeout_s)
                except Empty:
                    continue

                try:
                    msg = item.get("message", item.get("value", item))
                    self.send_message(self.action_topic, msg)

                    self.total_sent += 1
                    self.sent_since_heartbeat += 1

                    self.log.info("Sent message: topic=%s", self.action_topic)
                finally:
                    self.buy_signal_queue.task_done()

        finally:
            self.producer.flush(timeout=10)
            self.producer.close(timeout=10)
            self.log.info("Kafka producer loop stopped")


