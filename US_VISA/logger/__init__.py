import logging
import os
from datetime import datetime

LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

log_dir = "logs"

# ✅ Container-safe base directory
BASE_DIR = os.getcwd()

logs_path = os.path.join(BASE_DIR, log_dir, LOG_FILE)

os.makedirs(os.path.join(BASE_DIR, log_dir), exist_ok=True)

logging.basicConfig(
    filename=logs_path,
    format="[ %(asctime)s ] %(name)s - %(levelname)s - %(message)s",
    level=logging.DEBUG,
)
