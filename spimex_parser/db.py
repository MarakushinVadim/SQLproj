from sqlalchemy import create_engine, MetaData, select, exists
from sqlalchemy.orm import sessionmaker

from spimex_parser.config import Config
from spimex_parser.models import Base, SpimexTradingResultsBase


class Database:
    def __init__(self):
        self.engine = create_engine(Config.DB_URL)
        metadata = MetaData()
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def add_records(self, records: list):
        with self.Session() as session:
            try:
                query = select(exists().where(
                    SpimexTradingResultsBase.date == records[0].date
                ))
                if not session.scalar(query):
                    records_list = [
                        SpimexTradingResultsBase(
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
                        for obj in records
                    ]
                    session.add_all(records_list)
                    session.commit()
            except Exception as e:
                print(f"Ошибка при записи в БД: {e}")
                session.rollback()
                raise e
