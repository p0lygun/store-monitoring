from pathlib import Path


def clean_store_status_csv():
    """Clean store_status.csv"""
    file_path = Path(__file__).parent / 'csv' / 'store_status.csv'
    clean_file_path = Path(__file__).parent / 'csv' / 'store_status_clean.csv'

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
            ).replace('active', '1').replace(' UTC', '')
            clean_file.write(line)


def clean_csv_files():
    """Clean all csv files"""
    clean_store_status_csv()


if __name__ == '__main__':
    clean_store_status_csv()
