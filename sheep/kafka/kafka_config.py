"""
kafka_config.py

Load and build a KafkaProducer from a simple .properties file.

Supported keys:
- bootstrap.servers
- topic  (also accepts topic-name for convenience)
"""

from __future__ import annotations

import configparser
import json
import logging
from pathlib import Path
from typing import Any, Dict, Tuple

from kafka import KafkaProducer

log = logging.getLogger(__name__)


def load_kafka_config(kafka_properties_path: str) -> Tuple[Dict[str, Any], str]:
    p = Path(kafka_properties_path)
    if not p.exists():
        raise FileNotFoundError(f"Kafka properties file not found: {p}")

    # Read file, prepend a fake section header so configparser can parse it
    text = p.read_text(encoding="utf-8")
    if not text.lstrip().startswith("["):
        text = "[kafka]\n" + text

    parser = configparser.ConfigParser(interpolation=None)
    parser.optionxform = str  # preserve key casing if needed
    parser.read_string(text)

    cfg = parser["kafka"]

    bs = cfg.get("bootstrap.servers")
    if not bs:
        raise ValueError("Missing required property: bootstrap.servers")

    producer_kwargs: Dict[str, Any] = {
        "bootstrap_servers": [x.strip() for x in bs.split(",") if x.strip()],
        "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    }

    return producer_kwargs


def build_kafka_producer(producer_kwargs: Dict[str, Any]) -> KafkaProducer:
    try:
        producer = KafkaProducer(**producer_kwargs)
        log.info("Producer Created")
        return producer
    except Exception as e:
        log.exception("Failed to create KafkaProducer")
        raise RuntimeError("Failed to create KafkaProducer") from e

