from sqlalchemy import URL


class Config:
    USER_NAME = '****'  # Введите имя пользователя постгрес
    PASSWORD = "****" # Введите пароль пользователя постгрес
    DATABASE_NAME = '****'  # Введите название базы данных
    DB_URL = URL.create(
        drivername="postgresql",
        username=USER_NAME,
        password=PASSWORD,
        host="localhost",
        port=5432,
        database=DATABASE_NAME
    )
