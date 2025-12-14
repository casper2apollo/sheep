""" main.py

start all of casper's tasks

"""
import logging
import yaml
import threading
from queue import Queue

from src.casper_engine.casper_engine import CasperEngine
from src.casper_output.casper_output import CasperOutput
from src.kafka.kafka_utils import build_kafka_producer


log = logging.getLogger(__name__)



def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}



def main():
    
    logging.basicConfig(
        level=logging.INFO,  # or DEBUG
        format="%(asctime)s | %(levelname)s | %(name)s | %(threadName)s | %(message)s",
    )
    cfg = load_config("sheep/config/config.yaml")
    kafka_producer = build_kafka_producer(cfg.get("kafka_properties_path"))
    
    # --- shared queues ---
    # engine -> output
    output_queue: Queue = Queue(maxsize=cfg.get("queue_maxsize", 100_000) or 100_000)
    heartbeat_queue: Queue = Queue(maxsize=cfg.get("queue_maxsize", 100_000) or 100_000)
    
    
    
    # start casper engine thread in background (reads input queue and writes output queue)
    casper_engine = CasperEngine(heartbeat_queue=heartbeat_queue, output_queue=output_queue, cfg=cfg, log=log)
    casper_engine_thread = threading.Thread(
        target=casper_engine.run,
        name="CasperEngine",
        daemon=True,   # exits when main exits
    )
    casper_engine_thread.start()
    log.info("Initialized casper engine.")
    
    
    
    # start casper output thread in background (reads output queue)
    casper_output = CasperOutput(output_queue=output_queue, heartbeat_queue=heartbeat_queue, kafka_producer=kafka_producer, cfg=cfg, log=log)
    casper_output_thread = threading.Thread(
        target=casper_output.run_action,
        name="CasperOutput",
        daemon=True,   # exits when main exits
    )
    casper_output_thread.start()
    log.info("Initialized casper output.")
    
    
    # this keeps main on
    casper_output.run_heart()
    
    

if __name__ == "__main__":
    main()
