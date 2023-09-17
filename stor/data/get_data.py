from pathlib import Path
import asyncio
import aiohttp
from loguru import logger

files = {
    'store_status': '1UIx1hVJ7qt_6oQoGZgb8B3P2vd1FD025',
    'menu_hours': '1va1X3ydSh-0Rt1hsy2QSnHRA4w57PcXg',
    'time_zone_info': '101P9quxHoMZMZCVWQ5o-shonk2lgK1-o'
}


async def download_file(url, file_name):
    async with aiohttp.ClientSession() as session:
        file_path = Path(__file__).parent / 'csv' / f'{file_name}.csv'
        if file_path.exists():
            logger.debug(f"{file_name}.csv already exists")
            return
        response = await session.get(url)
        with open(file_path, 'w') as f:
            f.write(await response.text())

        logger.info(f"Downloaded {file_name}.csv")


async def execute_download():
    tasks = []
    for file_name, file_id in files.items():
        url = f"https://drive.google.com/uc?export=download&id={file_id}&confirm=1"
        tasks.append(download_file(url, file_name))
    await asyncio.gather(*tasks)


def get_csv_files():
    asyncio.run(execute_download())


if __name__ == '__main__':
    get_csv_files()

