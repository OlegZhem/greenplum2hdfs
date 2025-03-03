# src/__init__.py
import logging
import sys
from pathlib import Path
import os

# Determine the project root (parent of the 'src' directory)
_project_root = Path(__file__).resolve().parent.parent

# Create the 'logs' directory if it doesn't exist
_log_dir = _project_root / "logs"
_log_dir.mkdir(exist_ok=True)

# Create a formatter
_formatter = logging.Formatter('[%(asctime)s] %(message)s')
_formatter.default_msec_format = '%s.%03d'

# Create handlers
_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(_formatter)

_log_file = _log_dir / "greenplum2hdfs.log"
_file_handler = logging.FileHandler(_log_file)
_file_handler.setFormatter(_formatter)

# Configure the root logger for the entire package
def _configure_logging():
    # Configure the package-level logger
    logger = logging.getLogger(__name__)  # Logger name = "src"
    logger.setLevel(logging.DEBUG)
    logger.addHandler(_stdout_handler)
    logger.addHandler(_file_handler)

# Call the configuration function when the package is imported
_configure_logging()