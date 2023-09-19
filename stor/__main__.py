from loguru import logger

if True:
    from dotenv import load_dotenv
    logger.debug(f"Loaded Environment Variables {load_dotenv()}")

from .data.get_data import get_csv_files
from .db import create_connection



def app():
    logger.info("Starting stor")
    logger.debug("Getting csv files")
    get_csv_files()
    logger.debug("Connecting to database")
    connection = create_connection()
    logger.info("Starting Server...")
    # todo: start server
    logger.info("Server started")


if __name__ == '__main__':
    app()
