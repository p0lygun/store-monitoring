import os
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
CSV_DIR = PROJECT_DIR / 'data' / 'csv'
REPORT_CACHE_DIR = PROJECT_DIR / 'data' / 'report_cache'
DEBUG = os.environ.get('DEBUG', False) == 'True'
GENERATING_REPORTS = False


def ensure_project_directories_exists():
    """Ensures that project directories exist"""
    for directory in [
        PROJECT_DIR,
        CSV_DIR,
        REPORT_CACHE_DIR
    ]:
        directory.mkdir(exist_ok=True)
