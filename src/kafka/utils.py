# src/kafka/kafka_config.py

import os
import configparser
import logging
from json import JSONDecodeError

from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError

from src.kafka.schemas import KafkaProducerConfig, KafkaConsumerConfig



def env_override(props: dict, prop_key: str, env_key: str):
    v = os.getenv(env_key)
    if v is not None and v != "" and v != "None":
        props[prop_key] = v
    
    return props

def load_kafka_properties(file_path: str) -> dict:
    """load .properties file and flatten into a single dict."""
    config = configparser.ConfigParser()
    config.read(file_path)
    
    props = {}
    for section in config.sections():
        props.update(dict(config.items(section)))
        
    return props



def build_kafka_producer(file_path: str):
    """build KafkaProducer from kafka.properties file."""
    props = load_kafka_properties(file_path)
        
    # allow env overrides
    props = env_override(props, "bootstrap_servers", "KAFKA_PRODUCER_BOOTSTRAP_SERVERS")
    props = env_override(props, "topic", "KAFKA_OUTPUT_TOPIC")

    config = KafkaProducerConfig(**props)
    producer = KafkaProducer(
        **config.model_dump(exclude_none=True, exclude_unset=True),
        key_serializer=config.key_serializer,
        value_serializer=config.value_serializer
    )
    topic = props.get("topic")

    return producer, topic



def build_kafka_consumer(file_path: str):
    """builds KafkaConsumer from kafka.properties file."""
    props = load_kafka_properties(file_path)
    
    props = env_override(props, "bootstrap_servers", "KAFKA_CONSUMER_BOOTSTRAP_SERVERS")
    props = env_override(props, "group_id", "KAFKA_CONSUMER_GROUP_ID")
    props = env_override(props, "topic", "KAFKA_INPUT_TOPIC")
    
    config = KafkaConsumerConfig(**props)
    topic = props.get("topic")
    consumer = KafkaConsumer(topic, **config.model_dump())
    
    return consumer, topic



def get_next_message_batch(
    consumer: KafkaConsumer, timeout_ms=500
) -> list[ConsumerRecord]:
    """retrieve the next batch of messages from kafka."""
    records = consumer.poll(timeout_ms=timeout_ms)
    messages: list[ConsumerRecord] = []
    for msgs in records.values():
        messages.extend(msgs)
    return messages



def commit_batch(consumer: KafkaConsumer, batch: list[ConsumerRecord], logger=None):
    """Commit the offset of the latest message in the last batch."""
    if not batch:
        return

    if logger is None:
        logger = logging.getLogger(__name__)

    last_msg = batch[-1]
    tp = TopicPartition(last_msg.topic, last_msg.partition)
    consumer.commit(offsets={tp: OffsetAndMetadata(last_msg.offset + 1, None, -1)})
    logger.info(f"Committed offset {last_msg.offset+1} for partition {last_msg.partition}")
    


def send_message(producer: KafkaProducer, topic, message, logger=None):
    """send a message to kafka and flush the producer."""
    if not message:
        return
    
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        producer.send(topic, message)
        producer.flush()
        logger.info(f"Sent message to {topic}")
    except KafkaError as e:
        logger.error(f"Kafka error while sending batch: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error while sending message: {e}", exc_info=True)
        


def process_record(record: ConsumerRecord, logger: logging.Logger, max_attempts: int = 3):
    """Process an individual ConsumerRecord from a Kafka stream."""
    kafka_data = record.value
    
    if not isinstance(kafka_data, list) or not all(isinstance(item, dict) for item in kafka_data):
        logger.warning(f"Skipping invalid message: Expected list[dict], got {type(kafka_data).__name__}")
        return None
    
    return kafka_data
