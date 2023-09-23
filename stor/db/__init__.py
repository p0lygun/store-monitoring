import os
import pathlib

import psycopg2
from psycopg2 import sql, extras as pg_extras
from loguru import logger
from typing import TYPE_CHECKING

from ..config import CSV_DIR

if TYPE_CHECKING:
    from psycopg2.extensions import connection, cursor

pg_extras.register_uuid()
DB_CONFIG = {
    'user': os.getenv('DB_USERNAME'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'dbname': os.getenv('DB_DATABASE'),
    'cursor_factory': pg_extras.DictCursor
}
logger.debug(f"DB_CONFIG: {DB_CONFIG}")


def is_table_empty(cur: 'cursor', table_name: str) -> bool:
    # todo: find why pycharm is being a dick and complains "OJ expected, got 'table_name'"
    query = sql.SQL('SELECT TRUE FROM {table_name} LIMIT 1').format(
        table_name=sql.Identifier(table_name)
    )
    cur.execute(query)
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
        # todo: should I add a column for timezone?
        #  as a foreign key? as time_zone table is populated before this table
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


def init_cache_table(conn: 'connection') -> bool:
    """Create Table for cache"""
    with conn.cursor() as cur:
        # UUID is used as a key to store the data in disk
        # If cache exists it will be in /data/report_cache/{UUID}
        # todo: add a job that clears tabel if cache is not in /data/report_cache
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS report_cache (
                UUID uuid PRIMARY KEY not null,
                generating BOOLEAN not null DEFAULT true,
                start_timestamp_utc timestamptz not null,
                end_timestamp_utc timestamptz default null
            );
            """
        )
        conn.commit()
        logger.debug("Created cache table")
    return True


def init_settings_table(conn: 'connection') -> bool:
    """Create Table for settings"""
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                setting_name VARCHAR(255) PRIMARY KEY not null,
                setting_value VARCHAR(255) not null
            );
            """
        )
        conn.commit()
        logger.debug("Created settings table")
    return True


def init_db(conn: 'connection') -> bool:
    """Create Tables"""

    # todo: add error handling and what is bs checking for bool when the function may
    #  not return a bool

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

    if not init_cache_table(conn):
        logger.error("Unable to initialize cache table")
        return False

    if not init_settings_table(conn):
        logger.error("Unable to initialize settings table")
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


def populate_settings_table(conn: 'connection'):
    """Populates settings table"""
    with conn.cursor() as cur:
        logger.info("Populating settings table")
        cur.execute(
            """
            INSERT INTO settings (setting_name, setting_value)
            VALUES ('csv_data_changed', 'true')
            ON CONFLICT DO NOTHING;
            """
        )
        conn.commit()
        logger.debug("Populated settings table")


def get_settings(conn: 'connection', setting_name: str) -> dict:
    """Returns settings as a dict"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT setting_value
            FROM settings
            where setting_name = %s
            """,
            (setting_name,)
        )
        return cur.fetchone()


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
    populate_settings_table(conn)
    if get_settings(conn, 'csv_data_changed') == ['true']:
        logger.info("Populating data tables")
        populate_store_status(conn, SQL_STRING, CSV_DIR / 'store_status_clean.csv')
        populate_time_zone_table(conn, SQL_STRING, CSV_DIR / 'time_zone_info_clean.csv')
        populate_menu_hours_table(conn, SQL_STRING, CSV_DIR / 'menu_hours_clean.csv')
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE settings
                SET setting_value = 'false'
                WHERE setting_name = 'csv_data_changed';
                """
            )
            conn.commit()
        logger.info("Populated data tables")
