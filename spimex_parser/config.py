class Config:
    USER_NAME = ""  # Введите имя пользователя постгрес
    PASSWORD = ""  # Введите пароль пользователя постгрес
    DATABASE_NAME = ""  # Введите название базы данных
    DB_URL = f"postgresql+psycopg2://{USER_NAME}:{PASSWORD}@localhost:5432/{DATABASE_NAME}"