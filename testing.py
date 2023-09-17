import csv
from collections import Counter

store_counter = Counter()

with open("store_status.csv", mode="r", encoding="utf-8") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    line_count = 0
    for row in csv_reader:

        if line_count == 0:
            print(f'Column names are {", ".join(row)}')
            line_count += 1            
        else:
            store_counter[row["store_id"]] += 1
            line_count += 1

print(len(store_counter), store_counter.most_common(1))

 