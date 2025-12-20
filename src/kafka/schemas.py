# src/kafka/kafka_schemas.py

import json
from typing import Any, Callable, Optional

from pydantic import BaseModel, Field


class KafkaBaseConfig(BaseModel):
    
    bootstrap_servers: str = Field(..., description="Kafka bootstrap server(s)")
    security_protocol: Optional[str] = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_plain_username: Optional[str] = None
    sasl_plain_password: Optional[str] = None
    


class KafkaConsumerConfig(KafkaBaseConfig):
    
    group_id: str = Field(..., description="Consumer group ID")
    auto_offset_reset: str = Field("latest", description="Offset reset policy")
    enable_auto_commit: bool = Field(False, description="Enable/disable auto-commit of offsets")
    
    value_deserializer: Callable[[bytes], Any] = Field(
        default=lambda v: json.loads(v.decode("utf-8")),
        description="Deserializer for message values (defaults to JSON)",
    )
    
    

class KafkaProducerConfig(KafkaBaseConfig):
    
    acks: str = Field("all", description="Producer acknowledgement level")
    linger_ms: Optional[int] = 5
    retries: Optional[int] = 3
    client_id: Optional[str] = None
    
    key_serializer: Callable[[Any], bytes] = Field(
        default=lambda k: str(k).encode("utf-8"),
        description="Serializer for message keys (defaults to UTF-8 strings)"
    )
    value_serializer: Callable[[Any], bytes] = Field(
        default=lambda v: json.dumps(v).encode("utf-8"),
        description="Serializer for message values (defaults to JSON)",
    )
