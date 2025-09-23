import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from spimex_parser.models import Trade


class SpimexWebParser:
    def __init__(self, file_path: str, page_number: int = 1):
        self.file_path = file_path
        self.exel_file = None
        self.date = None
        self.trade_list = []
        self.page_number = page_number
        self.links_pattern = re.compile(r"^/upload/reports/oil_xls/oil_xls_202([543]).*")
        self.url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{self.page_number}"
        self.links = []

    def get_links(self):
        try:
            response = requests.get(self.url)
            html = response.text
            soup = BeautifulSoup(html, "html.parser")
            links = soup.find_all(
                "a",
                attrs={
                    "class": "accordeon-inner__item-title link xls",
                    "href": re.compile(self.links_pattern),
                },
            )
            self.page_number += 1
            return links
        except Exception as e:
            print(f"Ошибка при получении ссылок: {e}")
            self.page_number += 1
            return []

    def download_file(self, link):
        link_url = f"https://spimex.com{link['href']}"
        try:
            response = requests.get(link_url)
            response.raise_for_status()

            with open(self.file_path, "wb") as f:
                f.write(response.content)
        except Exception as e:
            print(f"Ошибка при скачивании файла: {e}")


    def parse(self):
        exel_file = pd.read_excel(self.file_path)
        self.date = datetime.strptime(
            exel_file.iloc[2]["Форма СЭТ-БТ"].replace("Дата торгов: ", ""),
            "%d.%m.%Y",
        )
        row_number = (
                int(
                    exel_file[
                        exel_file["Форма СЭТ-БТ"] == "Единица измерения: Метрическая тонна"
                        ].index[0]
                )
                + 2
        )
        exel_file = pd.read_excel(self.file_path, usecols="B:F,O", skiprows=row_number)
        exel_file = exel_file[exel_file['Количество\nДоговоров,\nшт.'] != '-'].dropna()
        return exel_file

    def read_data(self):
        entries_list = self.parse()
        for entry in range(len(entries_list)):
            try:
                trade = Trade(
                    exchange_product_id=entries_list.iloc[entry]["Код\nИнструмента"],
                    exchange_product_name=entries_list.iloc[entry][
                        "Наименование\nИнструмента"
                    ],
                    oil_id=entries_list.iloc[entry]["Код\nИнструмента"][:4],
                    delivery_basis_id=entries_list.iloc[entry]["Код\nИнструмента"][
                                      4:7
                                      ],
                    delivery_basis_name=entries_list.iloc[entry]["Базис\nпоставки"],
                    delivery_type_id=entries_list.iloc[entry]["Код\nИнструмента"][
                        -1
                    ],
                    volume=int(entries_list.iloc[entry]["Объем\nДоговоров\nв единицах\nизмерения"]),
                    total=int(entries_list.iloc[entry]["Обьем\nДоговоров,\nруб."]),
                    count=int(entries_list.iloc[entry]["Количество\nДоговоров,\nшт."]),
                    date=self.date,
                )
                self.trade_list.append(trade)
            except Exception as e:
                print(f"Ошибка при обработке записи {entry}: {e}")
        return self.trade_list