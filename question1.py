from datetime import timedelta

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy import String, Float, Integer, ForeignKey, DateTime, Interval


class Base(DeclarativeBase):
    pass


class GenreBase(Base):
    __tablename__ = "genre"

    id: Mapped[int] = mapped_column(primary_key=True)
    genre_name: Mapped[str] = mapped_column(String(50))


class BookBase(Base):
    __tablename__ = "book"

    id: Mapped[int] = mapped_column(primary_key=True)
    book_name: Mapped[str] = mapped_column(String(50))
    price: Mapped[float] = mapped_column(Float)
    count: Mapped[int] = mapped_column(Integer)
    genre_id: Mapped[int] = mapped_column(ForeignKey("GenreBase.id"))
    author_id: Mapped[int] = mapped_column(ForeignKey("AuthorBase.id"))


class AuthorBase(Base):
    __tablename__ = "author"

    id: Mapped[int] = mapped_column(primary_key=True)
    author_name: Mapped[str] = mapped_column(String(50))


class CityBase(Base):
    __tablename__ = "city"

    id: Mapped[int] = mapped_column(primary_key=True)
    city_name: Mapped[str] = mapped_column(String(50))
    shipping_time: Mapped[timedelta] = mapped_column(Interval)


class ClientBase(Base):
    __tablename__ = "client"

    id: Mapped[int] = mapped_column(primary_key=True)
    client_name: Mapped[str] = mapped_column(String(50))
    client_email: Mapped[str] = mapped_column(String(50))
    client_city: Mapped[int] = mapped_column(ForeignKey("CityBase.id"))


class OrderBase(Base):
    __tablename__ = "order"

    id: Mapped[int] = mapped_column(primary_key=True)
    clients_wishes: Mapped[str] = mapped_column(String())
    client_id: Mapped[int] = mapped_column(ForeignKey("ClientBase.id"))


class OrderBookBase(Base):
    __tablename__ = "order_book"

    id: Mapped[int] = mapped_column(primary_key=True)
    count: Mapped[int] = mapped_column(Integer)
    book_id: Mapped[int] = mapped_column(ForeignKey("BookBase.id"))
    order_id: Mapped[int] = mapped_column(ForeignKey("OrderBase.id"))


class StepBase(Base):
    __tablename__ = "step"

    id: Mapped[int] = mapped_column(primary_key=True)
    step_name: Mapped[str] = mapped_column(String(50))


class OrderStepBase(Base):
    __tablename__ = "order_step"

    id: Mapped[int] = mapped_column(primary_key=True)
    order_start: Mapped[DateTime] = mapped_column(DateTime)
    order_end: Mapped[DateTime] = mapped_column(DateTime)
    step_id: Mapped[int] = mapped_column(ForeignKey("StepBase.id"))
    client_id: Mapped[int] = mapped_column(ForeignKey("ClientBase.id"))
