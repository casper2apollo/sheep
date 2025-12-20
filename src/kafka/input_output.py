# src/kafka/input_output.py

import time
from logging import Logger
from kafka import KafkaConsumer, KafkaProducer
from queue import Queue, Empty, Full

from src.kafka.utils import (
    get_next_message_batch, process_record, commit_batch,
    send_message
)



class CasperInput:
    
    def __init__(self, input_queue: Queue, kafka_consumer: KafkaConsumer, log: Logger, cycle_interval: int = 10):
        self.input_queue = input_queue
        self.kafka_consumer = kafka_consumer
        self.log = log
        self.cycle_interval = cycle_interval
        self.running = True
        
    
    def run(self):
        """Get messages from kafka and add to internal queue"""
        while self.running:
            batch = get_next_message_batch(self.kafka_consumer)
            if not batch:
                self.log.debug(f"No messages received, sleeping for {self.cycle_interval} seconds")
                time.sleep(self.cycle_interval)
                continue
            
            messages = []
            for record in batch:
                try:
                    msg = process_record(record=record, logger=self.log)
                    if msg is not None:
                        messages.append(msg)
                except Exception as e:
                    self.log.error(f"Failed to process input record {record}: {e}")
                    
            if messages:
                try:
                    self.input_queue.put(messages, timeout=self.cycle_interval)
                    commit_batch(self.kafka_consumer, batch, logger=self.log)
                except Full:
                    self.log.error(f"Input Queue if full, not commiting batch")
            else:
                self.log.warning("All messages in the batch were invalid, skipping commit")
                





class CasperOutput:
    
    def __init__(self, output_queue: Queue, kafka_producer: KafkaProducer, topic: str, log: Logger, cycle_interval: int = 10):
        self.output_queue = output_queue
        self.kafka_producer = kafka_producer
        self.topic = topic
        self.log = log
        self.cycle_interval = cycle_interval
        self.running = True
        
        
    def run(self):
        """ 
        Get messages from interval queue and send them to kafka
        """
        while self.running:
            try:
                msg = self.output_queue.get(timeout=self.cycle_interval)
            except Empty:
                self.log.debug(f"No batch available (queue size={self.output_queue.qsize()}), sleeping for {self.cycle_interval} seconds")
                continue
            
            send_message(producer=self.kafka_producer, topic=self.topic, message=msg)