import logging
from datetime import datetime
from typing import Type, Dict, List, Any

import pandas as pd
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel

from app.database import session_scope, create_entity_s
from app.etl.transformer import transform_row, validate_entity
from app.models import Client, Driver, Persona
from app.etl.mappings import LINE

logger = logging.getLogger(__name__)


class ETLStats:
    def __init__(self):
        self.total_rows = 0
        self.success_count = 0
        self.error_count = 0
        self.errors: List[Dict[str, Any]] = []
        self.start_time = datetime.now()
        self.end_time = None

    def add_success(self):
        self.success_count += 1

    def add_error(self, row_num: int, error: str, row_data: Dict = None):
        self.error_count += 1
        self.errors.append({
            'row': row_num,
            'error': error,
            'data': row_data
        })

    def finish(self):
        self.end_time = datetime.now()

    def get_duration(self) -> float:
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0

    def __str__(self):
        duration = self.get_duration()
        return (
            f"${LINE}\n"
            f"ETL СТАТИСТИКА\n"
            f"${LINE}\n"
            f"Всего строк обработано: {self.total_rows}\n"
            f"Успешно загружено: {self.success_count}\n"
            f"Ошибок: {self.error_count}\n"
            f"Время выполнения: {duration:.2f} сек\n"
            f"${LINE}\n"
        )


def load_data(df: pd.DataFrame, model_class: Type[SQLModel],
              column_mapping: Dict[str, str]) -> ETLStats:
    stats = ETLStats()
    stats.total_rows = len(df)

    logger.info(f"Начало загрузки {stats.total_rows} строк в таблицу {model_class.__tablename__}")

    with session_scope() as session:
        for idx, row in df.iterrows():
            row_num = idx + 1

            try:
                data = transform_row(row, column_mapping)

                if not data:
                    logger.warning(f"Строка {row_num}: нет данных для загрузки")
                    stats.add_error(row_num, "Нет данных для загрузки", dict(row))
                    continue

                validate_entity(data)

                if model_class is Client or model_class is Driver:
                    logger.error(str(data))
                    persona = Persona(**data)
                    logger.error(str(persona))
                    create_entity_s(session, persona)
                    logger.error(str(persona))
                    data['id_клиента'] = persona.id

                entity = model_class(**data)
                create_entity_s(session, entity)
                stats.add_success()

                if stats.success_count % 10 == 0:
                    logger.info(f"Загружено {stats.success_count}/{stats.total_rows} строк")

            except IntegrityError as e:
                error_msg = f"Ошибка целостности данных: {str(e)}"
                logger.error(f"Строка {row_num}: {error_msg}")
                stats.add_error(row_num, error_msg, dict(row))
                session.rollback()

            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                logger.error(f"Строка {row_num}: {error_msg}")
                stats.add_error(row_num, error_msg, dict(row))
                session.rollback()

    stats.finish()

    return stats


def validate_data(df: pd.DataFrame, model_class: Type[SQLModel],
                  column_mapping: Dict[str, str]) -> ETLStats:
    stats = ETLStats()
    stats.total_rows = len(df)

    logger.info(f"Начало валидации {stats.total_rows} строк")

    for idx, row in df.iterrows():
        row_num = idx + 1
        try:
            data = transform_row(row, column_mapping)
            if not data:
                stats.add_error(row_num, "Нет данных", dict(row))
                continue

            validate_entity(data)
            model_class(**data)
            stats.add_success()

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            stats.add_error(row_num, error_msg, dict(row))

    stats.finish()

    return stats
