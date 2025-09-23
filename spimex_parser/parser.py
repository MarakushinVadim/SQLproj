import logging
import re
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

from spimex_parser.models import Trade


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class SpimexXlsDownloader:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def download_file(self, link):
        link_url = f"https://spimex.com{link['href']}"
        try:
            response = requests.get(link_url)
            response.raise_for_status()
            with open(self.file_path, "wb") as f:
                f.write(response.content)
            logging.info(f"Файл успешно скачан: {link_url}")

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP ошибка при скачивании файла: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Ошибка подключения: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            logging.error(f"Превышено время ожидания: {timeout_err}")
        except Exception as e:
            logging.error(f"Неизвестная ошибка при скачивании файла: {e}")


class SpimexWebParser:
    def __init__(self, file_path: str, page_number: int = 67):
        self.file_path = file_path
        self.exel_file = None
        self.date = None
        self.trade_list = None
        self.page_number = page_number
        self.links_pattern = re.compile(
            r"^/upload/reports/oil_xls/oil_xls_202([543]).*"
        )
        self.url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{self.page_number}"
        self.links = None

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
            self.url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{self.page_number}"
            logging.info(
                f"Получено {len(links)} ссылок с страницы {self.page_number - 1}"
            )
            return links

        except requests.exceptions.HTTPError as http_err:
            logging.error(f"HTTP ошибка при получении ссылок: {http_err}")
        except requests.exceptions.ConnectionError as conn_err:
            logging.error(f"Ошибка подключения при получении ссылок: {conn_err}")
        except requests.exceptions.Timeout as timeout_err:
            logging.error(
                f"Превышено время ожидания при получении ссылок: {timeout_err}"
            )
        except Exception as e:
            logging.error(f"Неизвестная ошибка при получении ссылок: {e}")

        self.page_number += 1
        self.url = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{self.page_number}"
        return []

    def parse(self):
        try:
            exel_file = pd.read_excel(self.file_path)
            self.date = datetime.strptime(
                exel_file.iloc[2]["Форма СЭТ-БТ"].replace("Дата торгов: ", ""),
                "%d.%m.%Y",
            )
            row_number = (
                int(
                    exel_file[
                        exel_file["Форма СЭТ-БТ"]
                        == "Единица измерения: Метрическая тонна"
                    ].index[0]
                )
                + 2
            )
            exel_file = pd.read_excel(
                self.file_path, usecols="B:F,O", skiprows=row_number
            )
            exel_file = exel_file[
                exel_file["Количество\nДоговоров,\nшт."] != "-"
            ].dropna()
            logging.info("Данные успешно прочитаны из Excel файла")
            return exel_file

        except FileNotFoundError:
            logging.error(f"Файл не найден: {self.file_path}")
        except pd.errors.EmptyDataError:
            logging.error("Файл пуст")
        except pd.errors.ParserError:
            logging.error("Ошибка парсинга Excel файла")
        except KeyError as e:
            logging.error(f"Отсутствующий столбец в Excel файле: {e}")
        except ValueError as e:
            logging.error(f"Ошибка формата даты: {e}")
        except Exception as e:
            logging.error(f"Неизвестная ошибка при парсинге Excel: {e}")

        return None

    def read_data(self):
        self.trade_list = []
        entries_list = self.parse()
        for entry in range(len(entries_list)):
            try:
                trade = Trade(
                    exchange_product_id=entries_list.iloc[entry]["Код\nИнструмента"],
                    exchange_product_name=entries_list.iloc[entry][
                        "Наименование\nИнструмента"
                    ],
                    oil_id=entries_list.iloc[entry]["Код\nИнструмента"][:4],
                    delivery_basis_id=entries_list.iloc[entry]["Код\nИнструмента"][4:7],
                    delivery_basis_name=entries_list.iloc[entry]["Базис\nпоставки"],
                    delivery_type_id=entries_list.iloc[entry]["Код\nИнструмента"][-1],
                    volume=int(
                        entries_list.iloc[entry][
                            "Объем\nДоговоров\nв единицах\nизмерения"
                        ]
                    ),
                    total=int(entries_list.iloc[entry]["Обьем\nДоговоров,\nруб."]),
                    count=int(entries_list.iloc[entry]["Количество\nДоговоров,\nшт."]),
                    date=self.date,
                )
                self.trade_list.append(trade)
            except ValueError as e:
                logging.error(f"Ошибка преобразования типов в записи {entry + 1}: {e}")
            except KeyError as e:
                logging.error(f"Отсутствующий ключ в записи {entry + 1}: {e}")
            except Exception as e:
                logging.error(f"Неизвестная ошибка в записи {entry + 1}: {e}")
        logging.info(f"Всего обработано {len(self.trade_list)} записей")
        return self.trade_list
