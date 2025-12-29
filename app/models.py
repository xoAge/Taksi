import re
from datetime import datetime
from typing import Optional
from decimal import Decimal

from fastapi import HTTPException
from sqlalchemy import Column, Unicode
from sqlalchemy.dialects import mssql
from sqlmodel import SQLModel, Field, Relationship


_phone_re = re.compile(r'^\+?\d{10,15}$')
_email_re = re.compile(r'^\S+?@\S+\.\S+$')


def _check(cond, key, msg):
    if not cond:
        raise HTTPException(
            status_code=400,
            detail=f"Поле '{key}' {msg}"
        )


def validate_phone(phone: str, key: str):
    _check(_phone_re.match(phone), key, "не является номером телефона")
    return phone


def validate_email(email: str, key: str):
    _check(_email_re.match(email), key, "не является электронным адресом")
    return email


def validate_past_date(dt: datetime, key: str):
    _check(dt < datetime.now(), key, "должно находиться в прошлом")
    return dt


def validate_positive(num, key: str):
    _check(isinstance(num, (int, float)) and num >= 0, key, "должны быть позитивным")
    return num


class Persona(SQLModel, table=True):
    __tablename__ = 'Персона'

    id: Optional[int] = Field(default=None, sa_column_kwargs={"name": "id_персоны"}, primary_key=True)
    name: str = Field(sa_column=Column("имя", Unicode(40)))
    phone: str = Field(sa_column=Column("телефон", Unicode(40)))
    registration_date: datetime = Field(sa_column_kwargs={"name": "дата_регистрации"})
    birthday: Optional[datetime] = Field(default=None, sa_column_kwargs={"name": "день_рождения"})

    client: Optional['Client'] = Relationship(back_populates='persona_rel', sa_relationship_kwargs={"uselist": False})
    driver: Optional['Driver'] = Relationship(back_populates='persona_rel', sa_relationship_kwargs={"uselist": False})
    geoposition: Optional['Geoposition'] = Relationship(back_populates='persona_rel', sa_relationship_kwargs={"uselist": False})
    written_reviews: list['Review'] = Relationship(
        back_populates='author_rel',
        sa_relationship_kwargs={"foreign_keys": "[Review.author_id]"}
    )
    received_reviews: list['Review'] = Relationship(
        back_populates='target_rel',
        sa_relationship_kwargs={"foreign_keys": "[Review.target_id]"}
    )


class Client(SQLModel, table=True):
    __tablename__ = 'Клиент'

    id: int = Field(foreign_key='Персона.id_персоны', sa_column_kwargs={"name": "id_клиента"}, primary_key=True)
    surname: Optional[str] = Field(default=None, sa_column=Column("фамилия", Unicode(40)))
    email: Optional[str] = Field(default=None, sa_column=Column("email", Unicode(40)))

    persona_rel: 'Persona' = Relationship(back_populates='client', sa_relationship_kwargs={"uselist": False})
    orders: list['Order'] = Relationship(back_populates='client_rel')
    payments: list['Payment'] = Relationship(back_populates='client_rel')


class Driver(SQLModel, table=True):
    __tablename__ = 'Водитель'

    id: int = Field(foreign_key='Персона.id_персоны', sa_column_kwargs={"name": "id_водителя"}, primary_key=True)
    surname: str = Field(sa_column=Column("фамилия", Unicode(40)))
    license_number: str = Field(sa_column=Column("номер_лицензии", Unicode(40)))
    is_working: bool = Field(sa_column_kwargs={"name": "работает"})
    car_id: int = Field(foreign_key='Автомобиль.id_авто', sa_column_kwargs={"name": "id_авто"})

    persona_rel: 'Persona' = Relationship(back_populates='driver', sa_relationship_kwargs={"uselist": False})
    car_rel: 'Car' = Relationship(back_populates='driver')
    orders: list['Order'] = Relationship(back_populates='driver_rel')


class Geoposition(SQLModel, table=True):
    __tablename__ = 'Геопозиция'

    persona_id: int = Field(foreign_key='Персона.id_персоны', sa_column_kwargs={"name": "id_персоны"}, primary_key=True)
    latitude: float = Field(sa_column_kwargs={"name": "широта"})
    longitude: float = Field(sa_column_kwargs={"name": "долгота"})
    mark_time: datetime = Field(sa_column_kwargs={"name": "время_отметки"})

    persona_rel: 'Persona' = Relationship(back_populates='geoposition', sa_relationship_kwargs={"uselist": False})


class Review(SQLModel, table=True):
    __tablename__ = 'Отзыв'

    author_id: int = Field(foreign_key='Персона.id_персоны', sa_column_kwargs={"name": "id_написавшего"}, primary_key=True)
    target_id: int = Field(foreign_key='Персона.id_персоны', sa_column_kwargs={"name": "id_цели"}, primary_key=True)
    rating: Optional[int] = Field(default=None, sa_column_kwargs={"name": "оценка"})
    comment: Optional[str] = Field(default=None, sa_column=Column("комментарий", Unicode(120)))
    creation_date: datetime = Field(sa_column_kwargs={"name": "дата_создания"})

    author_rel: 'Persona' = Relationship(
        back_populates='written_reviews',
        sa_relationship_kwargs={"foreign_keys": "[Review.author_id]"}
    )
    target_rel: 'Persona' = Relationship(
        back_populates='received_reviews',
        sa_relationship_kwargs={"foreign_keys": "[Review.target_id]"}
    )


class Car(SQLModel, table=True):
    __tablename__ = 'Автомобиль'

    id: Optional[int] = Field(default=None, sa_column_kwargs={"name": "id_авто"}, primary_key=True)
    brand: str = Field(sa_column=Column("марка", Unicode(40)))
    model: str = Field(sa_column=Column("модель", Unicode(40)))
    license_plate: str = Field(sa_column=Column("гос_номер", Unicode(40)))
    color: str = Field(sa_column=Column("цвет", Unicode(40)))
    year: Optional[int] = Field(default=None, sa_column_kwargs={"name": "год_выпуска"})
    is_personal: bool = Field(sa_column_kwargs={"name": "личный"})
    car_type_id: int = Field(foreign_key='Тип_авто.id_тип_авто', sa_column_kwargs={"name": "тип_авто"})

    car_type_rel: 'CarType' = Relationship(back_populates='cars')
    driver: Optional['Driver'] = Relationship(back_populates='car_rel', sa_relationship_kwargs={"uselist": False})


class CarType(SQLModel, table=True):
    __tablename__ = 'Тип_авто'

    id: Optional[int] = Field(default=None, sa_column_kwargs={"name": "id_тип_авто"}, primary_key=True)
    name: Optional[str] = Field(default=None, sa_column=Column("название", Unicode(40)))

    cars: list['Car'] = Relationship(back_populates='car_type_rel')


class OrderStatus(SQLModel, table=True):
    __tablename__ = 'Статус_заказа'

    id: Optional[int] = Field(default=None, sa_column_kwargs={"name": "id_статус_заказа"}, primary_key=True)
    value: str = Field(sa_column=Column("Значение_статуса", Unicode(40)))

    orders: list['Order'] = Relationship(back_populates='status_rel')


class Order(SQLModel, table=True):
    __tablename__ = 'Заказ'

    id: Optional[int] = Field(default=None, sa_column_kwargs={"name": "id_заказа"}, primary_key=True)
    order_time: datetime = Field(sa_column_kwargs={"name": "время_заказа"})
    arrival_time: Optional[datetime] = Field(default=None, sa_column_kwargs={"name": "время_прибытия"})
    departure_address: Optional[str] = Field(default=None, sa_column=Column("адрес_отправления", Unicode(120)))
    destination_address: str = Field(sa_column=Column("адрес_назанчения", Unicode(120)))
    distance_m: Optional[float] = Field(default=None, sa_column_kwargs={"name": "расстояние_м"})
    status_id: int = Field(foreign_key='Статус_заказа.id_статус_заказа', sa_column_kwargs={"name": "статус_заказа"})
    driver_id: Optional[int] = Field(default=None, foreign_key='Водитель.id_водителя', sa_column_kwargs={"name": "id_водителя"})
    client_id: int = Field(foreign_key='Клиент.id_клиента', sa_column_kwargs={"name": "id_клиента"})
    passenger_count: int = Field(sa_column_kwargs={"name": "колво_пассажиров"})
    has_animals: bool = Field(sa_column_kwargs={"name": "животные"})
    has_children: bool = Field(sa_column_kwargs={"name": "дети"})
    has_luggage: bool = Field(sa_column_kwargs={"name": "багаж"})

    status_rel: 'OrderStatus' = Relationship(back_populates='orders')
    driver_rel: Optional['Driver'] = Relationship(back_populates='orders')
    client_rel: 'Client' = Relationship(back_populates='orders')
    payment: Optional['Payment'] = Relationship(back_populates='order_rel', sa_relationship_kwargs={"uselist": False})


class Payment(SQLModel, table=True):
    __tablename__ = 'Оплата'

    order_id: int = Field(foreign_key='Заказ.id_заказа', sa_column_kwargs={"name": "id_заказа"}, primary_key=True)
    client_id: int = Field(foreign_key='Клиент.id_клиента', sa_column_kwargs={"name": "id_клиента"})
    amount: Decimal = Field(sa_column=Column("сумма", mssql.MONEY))
    payment_date: Optional[datetime] = Field(default=None, sa_column_kwargs={"name": "дата_оплаты"})
    payment_type: Optional[str] = Field(default=None, sa_column=Column("тип_оплаты", Unicode(40)))

    order_rel: 'Order' = Relationship(back_populates='payment', sa_relationship_kwargs={"uselist": False})
    client_rel: 'Client' = Relationship(back_populates='payments')
