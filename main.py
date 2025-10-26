import os
import sys
from utils.logger_utils import logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from run_ssl_monitor import main as run_ssl_job

if __name__ == "__main__":
    try:
        logger.info("🟢 Launching SSL Monitoring Job...")
        run_ssl_job()
        logger.info("✅ SSL Monitoring Job finished successfully!")
    except Exception as e:
        logger.error(f"❌ SSL Monitoring Job failed: {str(e)}", exc_info=True)
