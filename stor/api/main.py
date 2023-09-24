import uuid

from fastapi import FastAPI, BackgroundTasks, Request
from fastapi.responses import FileResponse
from fastapi.templating import Jinja2Templates
from loguru import logger
from fastapi_utils.tasks import repeat_every

from datetime import datetime, timezone

from ..db import create_connection, populate_db
from ..report import generate_report_for_all_stores, generate_total_report
from ..config import REPORT_CACHE_DIR, DEBUG, PROJECT_DIR
from ..data.get_data import get_csv_files, check_csv_exists
from .grapphing import get_graph_template_options_dict
app = FastAPI()
templates = Jinja2Templates(directory=PROJECT_DIR / "api" / "templates")


@app.get("/")
def index(request: Request, background_tasks: BackgroundTasks):

    return templates.TemplateResponse(
        "bokeh.html", get_graph_template_options_dict(request, background_tasks)
    )


@app.get("/trigger_report")
def trigger_report(background_tasks: BackgroundTasks):
    with create_connection() as conn:
        # check if there is a report is already being generated
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM report_cache
                WHERE generating = true;
                """
            )
            if report := cur.fetchone():
                return {"report_id": report[0]}

            report_id = uuid.uuid4()
            cur.execute(
                """
                INSERT INTO report_cache (UUID, generating, start_timestamp_utc)
                VALUES (%s, %s, %s);
                """,
                (report_id, True, datetime.now(timezone.utc)),
            )
            conn.commit()
            background_tasks.add_task(generate_report_for_all_stores, report_id)
            return {"report_id": report_id}


@app.get("/get_report")
def get_report(report_id: uuid.UUID):
    with create_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT * FROM report_cache
                WHERE UUID = %s;
                """,
                (report_id,),
            )
            report = cur.fetchone()
            if not report:
                return {"status": "Not Found"}

            report_file = REPORT_CACHE_DIR / f"{report_id}.csv"
            if not report_file.exists():
                # remove report from cache
                cur.execute(
                    """
                    DELETE FROM report_cache
                    WHERE UUID = %s;
                    """,
                    (report_id,),
                )
                conn.commit()
                return {"status": "Not Found"}

            if report["generating"]:
                return {
                    "status": f"generating",
                    "report_id": report["uuid"],
                }

            response = FileResponse(
                report_file,
                media_type="text/csv",
                headers={"status": "Completed"},
                filename=f"store_monitoring_{report_id}.csv"
            )
            return response


@repeat_every(seconds=60 * 60, logger=logger)
def poll_csv_data():
    get_csv_files(overwrite=not DEBUG)
    if check_csv_exists()[0]:
        with create_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE settings
                    SET setting_value = 'true'
                    WHERE setting_name = 'csv_data_changed';
                    """
                )
                conn.commit()
            populate_db(conn)


@app.get('/test')
def test_stuff():
    return {'ts': generate_total_report()}
