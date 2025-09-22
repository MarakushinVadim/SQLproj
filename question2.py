import re, requests, pandas as pd
import time
from dataclasses import dataclass
from datetime import datetime
from sqlalchemy import create_engine, String, Integer, DateTime, MetaData

from bs4 import BeautifulSoup
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column

start_time = time.time()


@dataclass(slots=True)
class Trade:
    exchange_product_id: str | None = None
    exchange_product_name: str | None = None
    oil_id: str | None = None
    delivery_basis_id: str | None = None
    delivery_basis_name: str | None = None
    delivery_type_id: str | None = None
    volume: int | None = None
    total: float | None = None
    count: int | None = None
    date: datetime | None = None
    created_on: datetime | None = datetime.now()
    updated_on: datetime | None = datetime.now()


class SpimexParser:
    def __init__(self, path: str):
        self.path = path
        self.exel_file = pd.read_excel(self.path)
        self.date = datetime.strptime(
            self.exel_file.iloc[2]["Форма СЭТ-БТ"].replace("Дата торгов: ", ""),
            "%d.%m.%Y",
        )
        self.trade_list = []

    def parse(self):
        exel_file = pd.read_excel(self.path)
        row_number = (
            int(
                exel_file[
                    exel_file["Форма СЭТ-БТ"] == "Единица измерения: Метрическая тонна"
                ].index[0]
            )
            + 4
        )
        data_columns = [
            "pass1",
            "exchange_product_id",
            "exchange_product_name",
            "delivery_basis_name",
            "volume",
            "total",
            "pass2",
            "pass3",
            "pass4",
            "pass5",
            "pass6",
            "pass7",
            "pass8",
            "pass9",
            "count",
        ]
        columns_to_drop = [
            "pass1",
            "pass2",
            "pass3",
            "pass4",
            "pass5",
            "pass6",
            "pass7",
            "pass8",
            "pass9",
        ]
        exel_file = pd.read_excel(
            self.path, skiprows=row_number, header=None, names=data_columns
        )
        exel_file = exel_file.drop(columns=columns_to_drop, axis=1)
        return exel_file

    def read_data(self):
        entries_list = self.parse()
        ignore_words = (
            "Маклер СПбМТСБ",
            "Маклер АО Петербургская Биржа",
            "Итого:",
            "Итого по секции:",
            "25/25",
            "24/24",
            "22/22",
            "21/21",
            "20/20",
            "19/19",
            "18/18",
            "17/17",
            "16/16",
            "15/15",
        )
        for entry in range(0, len(entries_list)):
            current_entry = entries_list.iloc[entry]
            if (
                current_entry["count"] != "-"
                and current_entry["exchange_product_id"] not in ignore_words
            ):
                trade = Trade(
                    exchange_product_id=entries_list.iloc[entry]["exchange_product_id"],
                    exchange_product_name=entries_list.iloc[entry][
                        "exchange_product_name"
                    ],
                    oil_id=entries_list.iloc[entry]["exchange_product_id"][:4],
                    delivery_basis_id=entries_list.iloc[entry]["exchange_product_id"][
                        4:7
                    ],
                    delivery_basis_name=entries_list.iloc[entry]["delivery_basis_name"],
                    delivery_type_id=entries_list.iloc[entry]["exchange_product_id"][
                        -1
                    ],
                    volume=int(entries_list.iloc[entry]["volume"]),
                    total=float(entries_list.iloc[entry]["total"]),
                    count=int(entries_list.iloc[entry]["count"]),
                    date=self.date,
                )
                self.trade_list.append(trade)
        return self.trade_list


class Base(DeclarativeBase):
    pass


class SpimexTradingResultsBase(Base):
    __tablename__ = "spimex_trading_results"

    id: Mapped[int] = mapped_column(primary_key=True)
    exchange_product_id: Mapped[str] = mapped_column(String(20))
    exchange_product_name: Mapped[str] = mapped_column(String())
    oil_id: Mapped[str] = mapped_column(String(4))
    delivery_basis_id: Mapped[str] = mapped_column(String(3))
    delivery_basis_name: Mapped[str] = mapped_column(String(50))
    delivery_type_id: Mapped[str] = mapped_column(String(1))
    volume: Mapped[int] = mapped_column(Integer)
    total: Mapped[int] = mapped_column(Integer)
    count: Mapped[int] = mapped_column(Integer)
    date: Mapped[datetime] = mapped_column(DateTime)
    created_on: Mapped[datetime] = mapped_column(DateTime)
    updated_on: Mapped[datetime] = mapped_column(DateTime)


engine = create_engine(
    "postgresql+psycopg2://postgres:Oblivion94$@localhost:5432/spimex_trading_results"
)
metadata = MetaData()
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

PAGE_NUMBER = 1

session_exceptions = 0

while True:
    URL = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{PAGE_NUMBER}"
    LINKS_PATTERN = re.compile(r"^/upload/reports/oil_xls/oil_xls_202([543]).*")
    file_path = "bulletin_file.xls"
    response = requests.get(URL)
    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all(
        "a",
        attrs={
            "class": "accordeon-inner__item-title link xls",
            "href": re.compile(LINKS_PATTERN),
        },
    )

    for i, link in enumerate(links):
        LINK_XLS_URL = f"https://spimex.com{link['href']}"
        print(f"Обрабатывается страница - {PAGE_NUMBER}, файл № - {i + 1}")
        try:
            response = requests.get(LINK_XLS_URL)
            response.raise_for_status()

            with open(file_path, "wb") as f:
                f.write(response.content)

        except requests.exceptions.RequestException as e:
            session_exceptions += 1
            print(f"Ошибка при скачивании файла: {e}")

        try:
            parser_spimex = SpimexParser(file_path)

            parser_data = parser_spimex.read_data()

        except Exception as e:
            session_exceptions += 1
            print(f"Ошибка при чтении файла: {e}")

        try:
            with engine.connect() as connection:
                for obj in parser_data:
                    record = SpimexTradingResultsBase(
                        exchange_product_id=obj.exchange_product_id,
                        exchange_product_name=obj.exchange_product_name,
                        oil_id=obj.oil_id,
                        delivery_basis_id=obj.delivery_basis_id,
                        delivery_basis_name=obj.delivery_basis_name,
                        delivery_type_id=obj.delivery_type_id,
                        volume=obj.volume,
                        total=obj.total,
                        count=obj.count,
                        date=obj.date,
                        created_on=obj.created_on,
                        updated_on=obj.updated_on,
                    )
                    session.add(record)
                session.commit()

        except Exception as e:
            session_exceptions += 1
            print(f"Ошибка при записи в базу данных: {e}")

    PAGE_NUMBER += 1

    if len(links) < 10:
        break

end_time = time.time()
duration = (end_time - start_time) / 60

total_count = session.query(SpimexTradingResultsBase).count()
print(f"Общее количество записей в базе данных - {total_count}")
print(f"Общее время выполнения программы - {duration} минут")
print(f"Общее количество ошибок за время выполнения программы - {session_exceptions}")
session.close()
