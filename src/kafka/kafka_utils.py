# src/kafka/kafka_config.py


import configparser
import logging
import uuid
import time
from json import JSONDecodeError

from kafka import KafkaConsumer, KafkaProducer, OffsetAndMetadata, TopicPartition
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaError

from src.kafka.kafka_schemas import KafkaProducerConfig, KafkaConsumerConfig



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
    config = KafkaProducerConfig(**props)
    producer = KafkaProducer(
        **config.model_dump(exclude_none=True, exclude_unset=True),
        key_serializer=config.key_serializer,
        value_serializer=config.value_serializer
    )
    action_topic        = props.get("action-topic")
    heartbeat_topic     = props.get("heartbeat-topic")
    
    return producer, action_topic, heartbeat_topic



def build_kafka_consumer(file_path):
    """builds KafkaConsumer from kafka.properties file."""
    props = load_kafka_properties(file_path)
    config = KafkaConsumerConfig(**props)
    trade_topic         = props.get("trade-topic")
    consumer = KafkaConsumer(trade_topic, **config.model_dump())
    return consumer, trade_topic



def get_next_batch(
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

    # Support both kafka-python signatures: (offset, metadata) and (offset, metadata, leader_epoch)
    try:
        offset_meta = OffsetAndMetadata(last_msg.offset + 1, None, -1)
    except TypeError:
        offset_meta = OffsetAndMetadata(last_msg.offset + 1, None)

    consumer.commit(offsets={tp: offset_meta})
    logger.info(f"Committed offset {last_msg.offset+1} for partition {last_msg.partition}")



def send_message(producer: KafkaProducer, topic, message: dict, logger=None):
    """Send a message to Kafka with uuid4 key and timestamp_ms, then flush the producer."""
    if not message:
        return
    
    if logger is None:
        logger = logging.getLogger(__name__)
    
    try:
        # Generate uuid4 key
        key = str(uuid.uuid4()).encode("utf-8")
        
        # Add timestamp_ms to the message payload
        message["timestamp_ms"] = int(time.time() * 1000)
        
        # Send with key and timestamp
        producer.send(
            topic,
            value=message,
            key=key,
            timestamp_ms=message["timestamp_ms"],
        )
        producer.flush()
        logger.info(f"Sent message to {topic} with key={key.decode()} and timestamp={message['timestamp_ms']}")
    except KafkaError as e:
        logger.error(f"Kafka error while sending batch: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Unexpected error while sending message: {e}", exc_info=True)



def process_record(record: ConsumerRecord, logger: logging.Logger, max_attempts: int = 3):
    """Process an individual ConsumerRecord from a Kafka stream."""
    attempts = 0
    processed = None

    while attempts < max_attempts and processed is None:
        try:
            attempts += 1

            kafka_data = record.value
            if not isinstance(kafka_data, list) or not all(isinstance(item, dict) for item in kafka_data):
                raise ValueError(
                    f"expected list[dict], got {type(kafka_data).__name__}: {kafka_data}"
                )

            # Right now we just return the validated payload as-is
            processed = kafka_data

        except (JSONDecodeError, ValueError) as e:
            logger.warning(f"Skipping invalid message: {e}")
            break  # not retriable
        except Exception as e:
            logger.error(f"Error processing message (attempt {attempts}): {e}", exc_info=True)
            # optional: small sleep/backoff here

    return processed
