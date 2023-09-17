from loguru import logger
from .data.get_data import get_csv_files


def app():
    logger.info("Starting stor")
    logger.debug("Getting csv files")
    get_csv_files()
    logger.info("Starting Server...")
    # todo: start server
    logger.info("Server started")


if __name__ == '__main__':
    app()
