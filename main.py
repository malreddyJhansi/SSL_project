import os
import sys

# Fix for Databricks (no _file_ variable)
BASE_DIR = os.getcwd()
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from utils.logger_utils import logger
from utils.spark_utility import *
from utils.pdf_report import *

logger.info("SSL Certificate Monitoring Job Started")
