import time

from spimex_parser.db import Database
from spimex_parser.models import SpimexTradingResultsBase
from spimex_parser.parser import SpimexWebParser

if __name__ == "__main__":
    start_time = time.time()

    db = Database()
    page_number = 1
    file_path = "bulletin_file.xls"

    parser = SpimexWebParser(file_path)

    while True:

        links = parser.get_links()
        for link in links:
            parser.download_file(link)
            records = parser.read_data()
            db.add_records(records)

        if len(links) < 10:
            break

    end_time = time.time()
    duration = (end_time - start_time) / 60

    total_count = db.Session.query(SpimexTradingResultsBase).count()
    print(f"Общее количество записей в базе данных - {total_count}")
    print(f"Общее время выполнения программы - {duration} минут")