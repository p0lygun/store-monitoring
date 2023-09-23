import uuid

import uvicorn
from loguru import logger

if True:
    from dotenv import load_dotenv
    logger.debug(f"Loaded Environment Variables {load_dotenv()}")


from .data.get_data import get_csv_files, check_csv_exists
from .data.clean_data import clean_csv_files
from .db import init_db, create_connection, populate_db
from .config import ensure_project_directories_exists


def app():
    logger.info("Starting stor")
    ensure_project_directories_exists()

    logger.debug("Getting csv files")
    get_csv_files()
    if not (ret := check_csv_exists())[0]:
        logger.error(ret[1])
        exit(1)
    clean_csv_files()

    logger.debug("Connecting to database")
    connection = create_connection()
    if not init_db(connection):
        logger.error("Unable to initialize database")
        exit(1)
    populate_db(connection)
    logger.info("Database initialized")
    connection.close()

    logger.info("Starting Server...")
    uvicorn.run(
        'stor.api.main:app',
        host="0.0.0.0",
        port=80,
        reload=True,
        reload_dirs=['/app/stor/api/']
    )
    logger.info("Server started")


def test_shit():
    from .report import generate_report_for_all_stores
    generate_report_for_all_stores(uuid.uuid4())


if __name__ == '__main__':
    app()
