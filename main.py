""" main.py

load config and run sheep_shear on every other hour

"""

import yaml
import threading
from queue import Queue

from sheep_shear.sheep_shear import SheepShear
from sheep_shear.kafka.kafka_producer import KafkaDataProducer


def load_config(path: str = "config.yaml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}



def main():
    cfg = load_config("config.yaml")

    sec_cfg = cfg.get("sec", {})
    path_cfg = cfg.get("path", {})
    kafka_cfg = cfg.get("kafka", {})  # where kafka_producer_path, sleep_time, heart_beat_freq live

    # shared queue
    buy_signal_queue: Queue = Queue(maxsize=kafka_cfg.get("queue_maxsize", 100_000) or 100_000)

    # start kafka producer thread in background (consumer of queue)
    kafka_producer = KafkaDataProducer(buy_signal_queue=buy_signal_queue, cfg=kafka_cfg)
    producer_thread = threading.Thread(
        target=kafka_producer.producer_loop,
        name="KafkaDataProducer",
        daemon=True,   # exits when main exits
    )
    producer_thread.start()

    # run shear loop (producer to queue) and push events onto the queue
    sheep_shear = SheepShear(sec_cfg=sec_cfg, path_cfg=path_cfg, buy_signal_queue=buy_signal_queue)
    sheep_shear.shear()



if __name__ == "__main__":
    main()
