""" kafka_producer.py

object will run on separate thread with access to a shared data queue.
it should read the queue and update the kafka topic.

"""
from __future__ import annotations

import logging
import time
import uuid
from queue import Queue, Empty
from typing import Any

from .kafka_config import build_kafka_producer, load_kafka_config

log = logging.getLogger(__name__)


class KafkaDataProducer:
    def __init__(self, buy_signal_queue: Queue, cfg: dict):
        producer_kwargs, default_topic = load_kafka_config(cfg["kafka_producer_path"])
        producer = build_kafka_producer(producer_kwargs=producer_kwargs)
        if producer is None:
            raise RuntimeError("Kafka producer failed to initialize (check kafka_producer_path and config).")
        if default_topic is None:
            raise RuntimeError("Kafka topic is not configured.")

        self.producer = producer
        self.default_topic = default_topic
        self.buy_signal_queue = buy_signal_queue

        # seconds
        self.sleep_time = float(cfg.get("sleep_time", 600.0)) # 10 mins = 600 sec
        # seconds between heartbeat logs
        self.heart_beat_freq = float(cfg.get("heart_beat_freq", 3600.0)) # 1 hour = 3600 sec

    def send(self, topic: str, message: Any) -> None:
        key_str = str(uuid.uuid4())
        key_bytes = key_str.encode("utf-8")
        self.producer.send(topic, key=key_bytes, value=message)

    def producer_loop(self, poll_timeout_s: float = 0.5) -> None:
        log.info("Kafka producer loop started")

        last_heartbeat = time.monotonic()

        try:
            while True:
                now = time.monotonic()

                # Heartbeat
                if self.heart_beat_freq > 0 and (now - last_heartbeat) >= self.heart_beat_freq:
                    try:
                        qsize = self.buy_signal_queue.qsize()
                    except Exception:
                        qsize = -1
                    log.info("KafkaDataProducer heartbeat: queue_size=%s topic=%s", qsize, self.default_topic)
                    last_heartbeat = now

                # Sleep throttle (optional)
                if self.sleep_time > 0:
                    time.sleep(self.sleep_time)

                # Poll queue
                try:
                    item = self.buy_signal_queue.get(timeout=poll_timeout_s)
                except Empty:
                    continue

                try:
                    topic = self.default_topic
                    message: Any = item

                    if isinstance(item, tuple) and len(item) == 2:
                        topic, message = item
                    elif isinstance(item, dict):
                        topic = item.get("topic", topic)
                        message = item.get("message", item.get("value", item))

                    if not topic:
                        raise ValueError("No topic provided and no default_topic configured")

                    self.send(topic, message)

                finally:
                    self.buy_signal_queue.task_done()

        finally:
            self.producer.flush(timeout=10)
            self.producer.close(timeout=10)
            log.info("Kafka producer loop stopped")
