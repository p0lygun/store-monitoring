from loguru import logger

if True:
    from dotenv import load_dotenv
    logger.debug(f"Loaded Environment Variables {load_dotenv()}")

from .data.get_data import get_csv_files, check_csv_exists
from .data.clean_data import clean_csv_files
from .db import init_db, create_connection, populate_db


def app():
    logger.info("Starting stor")
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
    logger.info("Starting Server...")
    # todo: start server
    logger.info("Server started")


if __name__ == '__main__':
    app()
