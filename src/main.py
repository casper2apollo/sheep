""" main.py

start all of casper's tasks

"""
import os
import sys
import logging
import yaml
import threading
from queue import Queue
from pathlib import Path

from src.casper_engine.casper_engine import CasperEngine
from src.kafka.input_output import CasperOutput
from src.kafka.utils import build_kafka_producer


if getattr(sys, "frozen", False): # PyInstaller bundle
    BASE_DIR = Path(".").resolve()
else:
    BASE_DIR = Path(__file__).resolve().parent

MAX_RETRIES = 3
MAX_ELAPSED_TIME = 300 # 5 min
MAX_QUEUE_SIZE = 10_000

PRODUCER_PATH = BASE_DIR / "config" / "kafka_producer.properties"

WATCHLIST_PATH = Path(
    os.getenv("WATCHLIST_PATH", BASE_DIR / "data")
) / "watchlist.json"



def validate_paths():
    """Ensure required files and directories exist."""
    for path in [PRODUCER_PATH]:
        if not path.exists():
            raise FileNotFoundError(f"Missing required conf file: {path}")
    
    if not (WATCHLIST_PATH.exists()):
        raise FileNotFoundError(f"Missing or empty signals database: {WATCHLIST_PATH}")
    

    
def ensure_watchlist(watchlist_path: Path):
    watchlist_path = Path(watchlist_path)
    watchlist_path.parent.mkdir(parents=True, exist_ok=True)



def setup_logger() -> logging.Logger:
    """Configure logging based on VERBOSE environment flag."""
    verbose = os.getenv("VERBOSE", "true").lower() == "true"
    log_level = logging.INFO if verbose else logging.WARNING
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("sheep")
    
    



def main():
    
    validate_paths()
    ensure_watchlist(WATCHLIST_PATH)
    log = setup_logger()
    
    cycle_interval_output = int(os.getenv("CYCLE_INTERVAL_OUTPUT", "10"))
    cycle_interval_engine = int(os.getenv("CYCLE_INTERVAL_ENGINE", "300")) # 5 mins = 300 sec
    sec_user_agent = str(os.getenv("SEC_USER_AGENT", "casper casper@2apollo.com"))
    
    # engine -> output
    output_queue: Queue = Queue(maxsize=MAX_QUEUE_SIZE)
    
    kafka_producer, producer_topic = build_kafka_producer(PRODUCER_PATH)
    
    # start casper output thread in background (reads output queue)
    casper_output = CasperOutput(output_queue=output_queue, kafka_producer=kafka_producer, topic=producer_topic, log=log, cycle_interval=cycle_interval_output)
    casper_output_thread = threading.Thread(
        target=casper_output.run,
        name="CasperOutput",
        daemon=True,   # exits when main exits
    )
    casper_output_thread.start()
    log.info("Initialized casper output.")
    
    
    # start casper engine thread in background (reads input queue and writes output queue)
    casper_engine = CasperEngine(output_queue=output_queue, sec_user_agent=sec_user_agent, watchlist_path=WATCHLIST_PATH, log=log, cycle_interval=cycle_interval_engine)
    casper_engine.run()
    
    
if __name__ == "__main__":
    main()
