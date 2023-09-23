from typing import TYPE_CHECKING, List, Tuple, Dict

import datetime

if TYPE_CHECKING:
    from psycopg2.extensions import connection, cursor


__all__ = [
    'get_all_stores',
    'get_store_status_log',
    'get_store_hours',
    'get_store_timezone',
    'get_max_timestamp',
]


def get_all_stores(conn: 'connection') -> List[int]:
    """Get all stores from database"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT store_id
            FROM store_status
            """
        )
        return [row[0] for row in cur.fetchall()]


def get_store_status_log(conn: 'connection', store_id: int, time_zone: str) -> List[
    Tuple[int, bool, datetime.datetime, datetime.datetime]
]:
    """Get store status log from database"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 
                *, 
                (timestamp_utc AT TIME ZONE '{timezone}')  AS timestamp_local
            FROM store_status
            WHERE store_id = {store_id}
            order by timestamp_utc
            """.format(store_id=store_id, timezone=time_zone)
        )
        return cur.fetchall()


def get_store_hours(conn: 'connection', store_id: int) -> Dict[
    int, Tuple[datetime.time, datetime.time]
]:
    """Get store hours from database"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT *
            FROM menu_hours
            WHERE store_id = {store_id}
            """.format(store_id=store_id)
        )
        store_hours = {
            day_of_week: (datetime.time(0, 0), datetime.time(23, 59)) for day_of_week in range(7)
        }
        for row in cur.fetchall():
            # day_of_week, start_time_local, end_time_local
            store_hours[row[1]] = (row[2], row[3])
        return store_hours


def get_store_timezone(conn: 'connection', store_id: int) -> str:
    """Get store timezone from database"""
    with conn.cursor() as cur:
        cur.execute(
            """
                SELECT COALESCE(
                (
                    SELECT timezone_str
                    from time_zone
                    where store_id = {store_id}
                ),
                    'America/Chicago'
                )
            """.format(store_id=store_id)
        )
        return cur.fetchone()[0]


def get_max_timestamp(conn: 'connection') -> datetime.datetime:
    """Get max timestamp from database"""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT MAX(timestamp_utc)
            FROM store_status
            """
        )
        return cur.fetchone()[0]

