from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict

import pandas as pd

from app.models import (
    validate_phone, validate_email, validate_past_date, validate_positive
)


def transform_row(row: pd.Series, column_mapping: Dict[str, str]) -> Dict[str, Any]:
    data = {}

    for file_col, model_field in column_mapping.items():
        if file_col in row.index:
            value = row[file_col]

            if pd.isna(value):
                continue

            transformed_value = _transform_value(value, model_field)

            if transformed_value is not None:
                data[model_field] = transformed_value

    return data


def _transform_value(value: Any, field_name: str) -> Any:
    if isinstance(value, str):
        value_lower = value.lower()
        if value_lower in ['true', 'да', '1', 'yes', 'истина']:
            return True
        elif value_lower in ['false', 'нет', '0', 'no', 'ложь']:
            return False

    if isinstance(value, str) and _is_date_field(field_name):
        try:
            return pd.to_datetime(value).to_pydatetime()
        except Exception:
            pass

    if isinstance(value, str):
        if field_name in ['amount', 'сумма']:
            try:
                return Decimal(value.replace(',', '.'))
            except (InvalidOperation, ValueError):
                pass

        try:
            if '.' not in value and ',' not in value:
                return int(value)
        except ValueError:
            pass

        try:
            return float(value.replace(',', '.'))
        except ValueError:
            pass

    return value


def _is_date_field(field_name: str) -> bool:
    date_keywords = ['date', 'time', 'дата', 'время']
    return any(keyword in field_name for keyword in date_keywords)


def validate_entity(data: Dict[str, Any]) -> None:
    if 'phone' in data:
        validate_phone(str(data['phone']), 'phone')

    if 'email' in data and data['email']:
        validate_email(data['email'], 'email')

    date_fields = ['registration_date', 'дата регистрации', 'mark_time', 'время_отметки',
                   'creation_date', 'дата_создания', 'order_time', 'время_заказа',
                   'arrival_time', 'время_прибытия', 'payment_date', 'дата_оплаты',
                   'birthday', 'день_рождения']

    for field in date_fields:
        if field in data and data[field]:
            date = datetime.strptime(data[field], "%d.%m.%Y")
            validate_past_date(date, field)

    positive_fields = ['distance_m', 'расстояние_м', 'passenger_count',
                       'колво_пассажиров', 'amount', 'сумма']
    for field in positive_fields:
        if field in data and data[field] is not None:
            validate_positive(data[field], field)

    if 'rating' in data and data['rating'] is not None:
        if not (1 <= data['rating'] <= 5):
            raise ValueError(f"Рейтинг должен быть от 1 до 5, получено: {data['rating']}")
