"""
kafka_config.py

Load and build a KafkaProducer from a simple .properties file.

Supported keys:
- bootstrap.servers
- topic
"""

from __future__ import annotations

import json
import logging
import configparser
from pathlib import Path
from typing import Any, Dict, Tuple, Optional

from kafka import KafkaProducer

log = logging.getLogger(__name__)


def load_kafka_config(kafka_properties_path: str) -> Tuple[Dict[str, Any], str]:
    """
    Returns:
      producer_kwargs: dict suitable for KafkaProducer(**producer_kwargs)
      topic: default topic to produce to
    """
    p = Path(kafka_properties_path)
    if not p.exists():
        raise FileNotFoundError(f"Kafka properties file not found: {p}")

    cfg = configparser.ConfigParser(interpolation=None)
    
    bs = cfg.get("bootstrap.servers")
    if not bs:
        raise ValueError("Missing required property: bootstrap.servers")

    topic = cfg.get("topic")
    if not topic:
        raise ValueError("Missing required property: topic")
    
    producer_kwargs: Dict[str, Any] = {
        "bootstrap_servers": [x.strip() for x in bs.split(",") if x.strip()],
        "key_serializer": lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    }
    
    return producer_kwargs, topic



def build_kafka_producer(producer_kwargs) -> Tuple[Optional[KafkaProducer], Optional[str]]:
    try:
        producer = KafkaProducer(**producer_kwargs)
        log.info("Producer Created")
        return producer
    except Exception:
        log.exception("Failed to create KafkaProducer")
        return None
