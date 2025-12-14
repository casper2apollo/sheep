""" casper_output.py

pass it on please

"""
import logging
import yaml
import time
import uuid
import threading
from queue import Queue

from src.kafka.kafka_utils import send_message


class CasperOutput:
    
    def __init__(self, output_queue: Queue, heartbeat_queue: Queue, cfg: dict, log: logging):
        self.output_queue = output_queue
        self.heartbeat_queue = heartbeat_queue
        self.cfg = cfg
        self.log = log
    
    
    
    def run_action(self):
        while True:
            if self.output_queue.qsize() > 0:
                message = self.output_queue.get()
                send_message(
                    topic=self.cfg.get("action_topic"),
                    message=message
                    )
            else:
                time.sleep(self.cfg.get("sleep_time"))
    
    
    
    def run_heart(self):
        while True:
            if self.heartbeat_queue.qsize() > 0:
                message = self.heartbeat_queue.get()
                send_message(
                    topic=self.cfg.get("heartbeat_topic"),
                    message=message
                    )
            else:
                time.sleep(self.cfg.get("heart_beat_time"))
