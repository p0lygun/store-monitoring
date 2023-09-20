import os
import pathlib

import psycopg2
from psycopg2 import sql
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


def is_table_empty(cur: 'cursor', table_name: str) -> bool:
    cur.execute(
        sql.SQL("SELECT TRUE FROM {table_name} LIMIT 1").format(table_name=sql.Identifier(table_name))
    )
    return cur.fetchone() is None


def create_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        logger.debug(f"Connected to {DB_CONFIG['dbname']}")
        return conn
    except Exception as e:
        logger.error(f"Unable to connect to {DB_CONFIG['dbname']}")
        logger.error(e)
        raise e


def init_store_status_table(conn: 'connection') -> bool:
    """Create Table for store status"""
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


def init_time_zone_table(conn: 'connection') -> bool:
    """Create Table for time zone"""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS time_zone (
                store_id BIGINT PRIMARY KEY not null,
                timezone_str VARCHAR(255) DEFAULT 'America/Chicago' not null
            );
            """
        )
        conn.commit()
        logger.debug("Created time_zone table")
    return True


def init_menu_hours_table(conn: 'connection') -> bool:
    """Create Table for menu hours"""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS menu_hours (
                store_id BIGINT not null,
                day_of_week SMALLINT not null,
                start_time_local TIME not null,
                end_time_local TIME not null,
                PRIMARY KEY (store_id, day_of_week)
            );
            """
        )
        conn.commit()
        logger.debug("Created menu_hours table")
    return True


def init_db(conn: 'connection') -> bool:
    """Create Tables"""

    # create table for store status
    if not init_store_status_table(conn):
        logger.error("Unable to initialize store_status table")
        return False

    if not init_time_zone_table(conn):
        logger.error("Unable to initialize time_zone table")
        return False

    if not init_menu_hours_table(conn):
        logger.error("Unable to initialize menu_hours table")
        return False

    return True


def populate_store_status(conn: 'connection', sql_string: str, file: pathlib.Path):
    with conn.cursor() as cur:
        if os.getenv('DEBUG', False) and not is_table_empty(cur, 'store_status'):
            logger.debug("Skipping populating of store_status table")
            return

        logger.info("Populating store_status table")
        with open(file, 'r') as f:
            cur.copy_expert(
                sql=sql_string.format(main_table='store_status'),
                file=f
            )
        conn.commit()
        logger.debug("Populated store_status table")


def populate_time_zone_table(conn: 'connection', sql_string: str, file: pathlib.Path):
    """Populates time_zone table"""
    with conn.cursor() as cur:
        if os.getenv('DEBUG', False) and not is_table_empty(cur, 'time_zone'):
            logger.debug("Skipping populating of time_zone table")
            return

        logger.info("Populating time_zone table")
        with open(file, 'r') as f:
            cur.copy_expert(
                sql=sql_string.format(main_table='time_zone'),
                file=f
            )
        conn.commit()
        logger.debug("Populated time_zone table")


def populate_menu_hours_table(conn: 'connection', sql_string: str, file: pathlib.Path):
    """Populates menu_hours table"""
    with conn.cursor() as cur:
        if os.getenv('DEBUG', False) and not is_table_empty(cur, 'menu_hours'):
            logger.debug("Skipping populating of menu_hours table")
            return

        logger.info("Populating menu_hours table")
        with open(file, 'r') as f:
            cur.copy_expert(
                sql=sql_string.format(main_table='menu_hours'),
                file=f
            )
        conn.commit()
        logger.debug("Populated menu_hours table")


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
    populate_store_status(conn, SQL_STRING, CSV_DIR / 'store_status_clean.csv')
    populate_time_zone_table(conn, SQL_STRING, CSV_DIR / 'time_zone_info_clean.csv')
    populate_menu_hours_table(conn, SQL_STRING, CSV_DIR / 'menu_hours_clean.csv')
