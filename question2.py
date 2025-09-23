# import re, requests
# import time
#
# from sqlalchemy import create_engine, MetaData
#
# from bs4 import BeautifulSoup
# from sqlalchemy.orm import sessionmaker
#
# from spimex_parser.models import Base, SpimexWebParser, SpimexTradingResultsBase
#
# start_time = time.time()
#
#
# # USER_NAME = "" # Введите имя пользователя постгрес
# # PASSWORD = "" # Введите пароль пользователя постгрес
# # DATABASE_NAME = "" #  Введите название базы данных
# #
# # engine = create_engine(
# #     f"postgresql+psycopg2://{USER_NAME}:{PASSWORD}@localhost:5432/{DATABASE_NAME}"
# # )
# # metadata = MetaData()
# # Base.metadata.create_all(engine)
# # Session = sessionmaker(bind=engine)
# # session = Session()
#
# PAGE_NUMBER = 1
#
# session_exceptions = 0
#
# while True:
#     # URL = f"https://spimex.com/markets/oil_products/trades/results/?page=page-{PAGE_NUMBER}"
#     # LINKS_PATTERN = re.compile(r"^/upload/reports/oil_xls/oil_xls_202([543]).*")
#     # file_path = "bulletin_file.xls"
#     # response = requests.get(URL)
#     # html = response.text
#     # soup = BeautifulSoup(html, "html.parser")
#     # links = soup.find_all(
#     #     "a",
#     #     attrs={
#     #         "class": "accordeon-inner__item-title link xls",
#     #         "href": re.compile(LINKS_PATTERN),
#     #     },
#     # )
#
#     # for i, link in enumerate(links):
#     #     LINK_XLS_URL = f"https://spimex.com{link['href']}"
#         print(f"Обрабатывается страница - {PAGE_NUMBER}, файл № - {i + 1}")
#         try:
#             response = requests.get(LINK_XLS_URL)
#             response.raise_for_status()
#
#             with open(file_path, "wb") as f:
#                 f.write(response.content)
#
#         except requests.exceptions.RequestException as e:
#             session_exceptions += 1
#             print(f"Ошибка при скачивании файла: {e}")
#
#         try:
#             parser_spimex = SpimexWebParser(file_path)
#
#             parser_data = parser_spimex.read_data()
#
#         except Exception as e:
#             session_exceptions += 1
#             print(f"Ошибка при чтении файла: {e}")
#
#         try:
#             with engine.connect() as connection:
#                 records = [
#                     SpimexTradingResultsBase(
#                         exchange_product_id=obj.exchange_product_id,
#                         exchange_product_name=obj.exchange_product_name,
#                         oil_id=obj.oil_id,
#                         delivery_basis_id=obj.delivery_basis_id,
#                         delivery_basis_name=obj.delivery_basis_name,
#                         delivery_type_id=obj.delivery_type_id,
#                         volume=obj.volume,
#                         total=obj.total,
#                         count=obj.count,
#                         date=obj.date,
#                         created_on=obj.created_on,
#                         updated_on=obj.updated_on,
#                     )
#                     for obj in parser_data
#                 ]
#                 session.add_all(records)
#                 session.commit()
#
#         except Exception as e:
#             session_exceptions += 1
#             print(f"Ошибка при записи в базу данных: {e}")
#             session.rollback()
#
#     PAGE_NUMBER += 1
#
