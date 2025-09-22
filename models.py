from dataclasses import dataclass
from datetime import datetime
from sqlalchemy import String, Integer, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import pandas as pd


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
