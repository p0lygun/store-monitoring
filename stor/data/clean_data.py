import shutil

from ..config import CSV_DIR


def clean_store_status_csv():
    """Clean store_status.csv"""
    file_path = CSV_DIR / 'store_status.csv'
    clean_file_path = CSV_DIR / 'store_status_clean.csv'

    if clean_file_path.exists():
        # todo: think about if we should regenerate the clean file or not
        return
    with open(file_path, 'r') as main_file, open(clean_file_path, 'w') as clean_file:
        for line in main_file:
            if line.startswith('store_id'):
                clean_file.write(line)
                continue
            # order of replace matters, as 'inactive' is a substring of 'active'
            line = line.replace(
                'inactive', '0'
            ).replace('active', '1')
            clean_file.write(line)


def clean_menu_hours_csv():
    """Clean menu_hours.csv"""
    file_path = CSV_DIR / 'menu_hours.csv'
    clean_file_path = CSV_DIR / 'menu_hours_clean.csv'

    if clean_file_path.exists():
        # todo: think about if we should regenerate the clean file or not
        return

    # Nothing to clean in menu_hours.csv, for now, just copy it over
    shutil.copyfile(file_path, clean_file_path)
    return


def clean_time_zone_info_csv():
    """Clean time_zone_info.csv"""
    file_path = CSV_DIR / 'time_zone_info.csv'
    clean_file_path = CSV_DIR / 'time_zone_info_clean.csv'

    if clean_file_path.exists():
        # todo: think about if we should regenerate the clean file or not
        return

    # Nothing to clean in time_zone_info.csv, for now, just copy it over
    shutil.copyfile(file_path, clean_file_path)
    return


def clean_csv_files():
    """Clean all csv files"""
    clean_store_status_csv()
    clean_menu_hours_csv()
    clean_time_zone_info_csv()


if __name__ == '__main__':
    clean_csv_files()
