import os
import psycopg2
from loguru import logger


DB_CONFIG = {
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_DATABASE'),
}
logger.debug(f"DB_CONFIG: {DB_CONFIG}")

def create_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.debug(f"Connected to {DB_CONFIG['dbname']}")
        return conn
    except Exception as e:
        logger.error(f"Unable to connect to {DB_CONFIG['dbname']}")
        logger.error(e)
        raise e


