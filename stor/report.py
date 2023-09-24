import uuid
from typing import List, Tuple, TYPE_CHECKING, Dict
from .db import create_connection
from .db.functions import *
from dataclasses import dataclass

from datetime import datetime, timedelta
import zoneinfo
from . import config
import csv

from loguru import logger

if TYPE_CHECKING:
    from psycopg2.extensions import connection


@dataclass
class StatusLogRow:
    """Dataclass for store status log row"""
    store_id: int
    is_open: bool
    timestamp_utc: datetime
    timestamp_local: datetime
    timezone: str

    def __post_init__(self):
        self.timestamp_local = self.timestamp_local.astimezone(zoneinfo.ZoneInfo(self.timezone))


def calculate_relative_report(
        store_status: List[StatusLogRow],
        store_hours: Dict[int, Tuple[datetime.time, datetime.time]],
        start_time: datetime,
        end_time: datetime,
        debug: bool = False
) -> Tuple[timedelta, timedelta]:
    """Calculate report between start_time and end_time"""

    store_status = [row for row in store_status if start_time <= row.timestamp_utc]
    if debug:
        logger.debug(store_status)
    if not store_status:
        return timedelta(), timedelta()

    uptime_timedelta = timedelta()
    downtime_timedelta = timedelta()

    last_status = store_status[0]

    for status_row in store_status[1:]:
        day_of_week = status_row.timestamp_local.weekday()
        start_time_local, end_time_local = store_hours[day_of_week]

        # check if status is within store hours
        if start_time_local <= status_row.timestamp_local.time() <= end_time_local:
            time_since_last_status = status_row.timestamp_utc - last_status.timestamp_utc
            # check if status changed
            if status_row.is_open != last_status.is_open:
                # check if status changed to open
                if status_row.is_open:
                    # add downtime
                    downtime_timedelta += time_since_last_status
                else:
                    # add uptime
                    uptime_timedelta += time_since_last_status

            else:
                if status_row.is_open:
                    # add uptime
                    uptime_timedelta += time_since_last_status
                else:
                    # add downtime
                    downtime_timedelta += time_since_last_status

        last_status = status_row

    time_since_last_status = end_time - last_status.timestamp_utc
    if debug:
        logger.debug(time_since_last_status)
    # check if last status is open
    if last_status.is_open:
        # add uptime
        uptime_timedelta += time_since_last_status
    else:
        # add downtime
        downtime_timedelta += time_since_last_status

    return uptime_timedelta, downtime_timedelta


def calculate_report_last_hour(
        store_status: List[StatusLogRow],
        store_hours: Dict[int, Tuple[datetime.time, datetime.time]],
        end_time: datetime
) -> Tuple[int, int]:
    """Calculate report for last hour"""

    uptime, downtime = calculate_relative_report(
        store_status,
        store_hours,
        end_time - timedelta(hours=1),
        end_time,
        debug=False
    )
    uptime = int(uptime.total_seconds()) // 60
    downtime = int(downtime.total_seconds()) // 60
    return uptime, downtime


def calculate_report_last_day(
        store_status: List[StatusLogRow],
        store_hours: Dict[int, Tuple[datetime.time, datetime.time]],
        end_time: datetime
) -> Tuple[int, int]:
    """Calculate report for day hour"""

    uptime, downtime = calculate_relative_report(
        store_status,
        store_hours,
        end_time - timedelta(days=1),
        end_time
    )
    uptime = int(uptime.total_seconds()) // 3600
    downtime = int(downtime.total_seconds()) // 3600

    return uptime, downtime


def calculate_report_last_week(
        store_status: List[StatusLogRow],
        store_hours: Dict[int, Tuple[datetime.time, datetime.time]],
        end_time: datetime
) -> Tuple[int, int]:
    """Calculate report for week hour"""

    uptime, downtime = calculate_relative_report(
        store_status,
        store_hours,
        end_time - timedelta(days=7),
        end_time
    )

    uptime = int(uptime.total_seconds()) // 3600
    downtime = int(downtime.total_seconds()) // 3600

    return uptime, downtime


def generate_report_for_store(
        conn: 'connection',
        store_id: int, end_time: datetime
) -> Dict[str, Tuple[int, int]]:
    """Generate report for store"""

    store_hours = get_store_hours(conn, store_id)
    timezone = get_store_timezone(conn, store_id)
    store_status = [
        StatusLogRow(store_id, status, timestamp_utc, timestamp_local, timezone)
        for store_id, status, timestamp_utc, timestamp_local in get_store_status_log(conn, store_id, timezone)
    ]
    report = {
        'store_id': store_id,
        'last_hour': calculate_report_last_hour(store_status, store_hours, end_time),
        'last_day': calculate_report_last_day(store_status, store_hours, end_time),
        'last_week': calculate_report_last_week(store_status, store_hours, end_time)
    }

    return report


def generate_report_for_all_stores(report_id: uuid.UUID):
    report_file = config.REPORT_CACHE_DIR / f'{report_id}.csv'
    if report_file.exists():
        return

    with open(report_file, 'w') as csv_file, create_connection() as conn:
        csv_file_writer = csv.writer(csv_file)
        csv_header = [
            'store_id',
            'uptime_last_hour',
            'uptime_last_day',
            'uptime_last_week',
            'downtime_last_hour',
            'downtime_last_day',
            'downtime_last_week'
        ]
        csv_file_writer.writerow(csv_header)

        max_timestamp = get_max_timestamp(conn)
        stores = get_all_stores(conn)
        logger.info(f"Generating report for {len(stores)} stores, for report {report_id}")
        for store in stores:
            report = generate_report_for_store(
                conn, store, max_timestamp
            )
            report_row = [
                report['store_id'],
                report['last_hour'][0],
                report['last_day'][0],
                report['last_week'][0],
                report['last_hour'][1],
                report['last_day'][1],
                report['last_week'][1]
            ]
            csv_file_writer.writerow(report_row)
        logger.info(f"Finished generating report for {len(stores)} stores, for report {report_id}")
        # update report cache
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE 
                    report_cache 
                SET generating = %s, end_timestamp_utc = %s
                WHERE uuid=%s 
                """,
                (False, datetime.utcnow(), report_id)
            )
            conn.commit()
        logger.info(f"Updated report cache for report {report_id}")


def generate_total_report():
    with create_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT 
                    MIN(timestamp_utc)
                FROM 
                    store_status
                """
            )
            min_timestamp = cursor.fetchone()
            if min_timestamp:
                min_timestamp = min_timestamp[0]
        all_stores = get_all_stores(conn)
        with open(config.REPORT_CACHE_DIR / 'total_report.csv', 'w') as csv_file:
            csv_file_writer = csv.writer(csv_file)
            csv_file_writer.writerow([
                'store_id', 'uptime', 'downtime'
            ])
            for store_id in all_stores:
                store_hours = get_store_hours(conn, store_id)
                timezone = get_store_timezone(conn, store_id)
                store_status = [
                    StatusLogRow(store_id, status, timestamp_utc, timestamp_local, timezone)
                    for store_id, status, timestamp_utc, timestamp_local in get_store_status_log(conn, store_id, timezone)
                ]
                uptime, downtime = calculate_relative_report(
                        store_status,
                        store_hours,
                        min_timestamp,
                        get_max_timestamp(conn)
                    )
                csv_file_writer.writerow([
                    store_id,
                    uptime.total_seconds(),
                    downtime.total_seconds()
                ])

    config.GENERATING_REPORTS = False
    return {
        'generated': True,
    }


def test_report_generation():
    store_id = 8139926242460185114
    with create_connection() as conn:
        max_timestamp = get_max_timestamp(conn)
        report = generate_report_for_store(
            conn, store_id, max_timestamp
        )
        print(report)
