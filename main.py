""" main.py

load config and run sheep_shear on every other hour

"""
import logging
import yaml
import threading
from queue import Queue

from sheep.sheep_buy_signal import SheepBuySignal
from sheep.kafka.kafka_producer import KafkaDataProducer

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

    sec_cfg = cfg.get("sec", {})
    path_cfg = cfg.get("path", {})
    kafka_cfg = cfg.get("kafka", {})  # where kafka_producer_path, sleep_time, heart_beat_freq live

    # shared queue
    buy_signal_queue: Queue = Queue(maxsize=kafka_cfg.get("queue_maxsize", 100_000) or 100_000)
    log.info("Initialized buy signal queue.")


    # start kafka producer thread in background (consumer of queue)
    kafka_producer = KafkaDataProducer(buy_signal_queue=buy_signal_queue, cfg=kafka_cfg, log=log)
    producer_thread = threading.Thread(
        target=kafka_producer.producer_loop,
        name="KafkaProducer",
        daemon=True,   # exits when main exits
    )
    producer_thread.start()
    log.info("Initialized kafka producer.")
    

    # run shear loop (producer to queue) and push events onto the queue
    sheep_buy_signal = SheepBuySignal(sec_cfg=sec_cfg, path_cfg=path_cfg, buy_signal_queue=buy_signal_queue, log=log)
    log.info("SHEARING SHEEP!")
    sheep_buy_signal.shear_loop()
    


if __name__ == "__main__":
    main()
