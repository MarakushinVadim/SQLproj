from dataclasses import dataclass
from datetime import datetime
from sqlalchemy import String, Integer, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


@dataclass(slots=True)
class Trade:
    exchange_product_id: str
    exchange_product_name: str
    oil_id: str
    delivery_basis_id: str
    delivery_basis_name: str
    delivery_type_id: str
    volume: int
    total: int
    count: int
    date: datetime
    created_on: datetime = datetime.now()
    updated_on: datetime = datetime.now()


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
