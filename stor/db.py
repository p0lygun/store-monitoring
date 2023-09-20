import os
import psycopg2
from loguru import logger
from typing import TYPE_CHECKING

from .config import CSV_DIR

if TYPE_CHECKING:
    from psycopg2.extensions import connection, cursor

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


def init_db(conn: 'connection') -> bool:
    """Create Tables"""

    # create table for store status
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS store_status (
                store_id BIGINT not null,
                status BOOLEAN not null,
                timestamp_utc timestamptz not null,
                PRIMARY KEY (store_id, timestamp_utc)
            );
            """
        )
        # create hypertable
        cur.execute(
            """
            SELECT create_hypertable (
                'store_status', 
                'timestamp_utc', 
                if_not_exists => TRUE
            );
            """
        )

        # create index on store_id and timestamp_utc
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_store_id_timestamp_utc
            ON store_status (store_id, timestamp_utc);
            """
        )
        conn.commit()
        logger.debug("Created store_status table")

    return True


def populate_db(conn: 'connection'):
    """Populate Tables"""
    SQL_STRING = """
                CREATE TEMP TABLE tmp_table 
                ON COMMIT DROP
                AS
                SELECT * 
                FROM {main_table}
                WITH NO DATA;
                
                COPY tmp_table from STDIN DELIMITER ',' CSV HEADER;
                
                INSERT INTO {main_table}
                SELECT *
                FROM tmp_table
                ON CONFLICT DO NOTHING;
                """.strip()

    cur: 'cursor'
    # Load store_status
    with conn.cursor() as cur:
        logger.info("Populating store_status table")
        with open(CSV_DIR / 'store_status_clean.csv', 'r') as f:
            cur.copy_expert(
                sql=SQL_STRING.format(main_table='store_status'),
                file=f
            )
        conn.commit()
        logger.debug("Populated store_status table")
