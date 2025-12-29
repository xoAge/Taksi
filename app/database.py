import logging
from contextlib import contextmanager

import pyodbc
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import sessionmaker

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s\t%(levelname)s:\t%(message)s',
)
logger = logging.getLogger(__name__)

engine = create_engine(
    "mssql+pyodbc://(localdb)\\MSSQLLocalDB/TAXI?"
    "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)
Session = sessionmaker(bind=engine)


@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
    except HTTPException:
        session.rollback()
        raise
    except (IntegrityError, pyodbc.IntegrityError) as e:
        session.rollback()
        logger.error(f"IntegrityError: {e}")
        raise HTTPException(
            status_code=400,
            detail=f"Ошибка целостности данных: {e}"
        )
    except SQLAlchemyError as e:
        session.rollback()
        logger.error(f"SQLAlchemyError: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера: {e}"
        )
    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Внутренняя ошибка сервера: {e}"
        )
    finally:
        session.close()


def create_entity(entity):
    with session_scope() as session:
        return create_entity_s(session, entity)


def get_entities(entity_class, key=None):
    with session_scope() as session:
        return get_entities_s(session, entity_class, key)


def update_entity(entity_class, key, update_data):
    with session_scope() as session:
        return update_entity_s(session, entity_class, key, update_data)


def delete_entity(entity_class, key):
    with session_scope() as session:
        return delete_entity_s(session, entity_class, key)


def create_entity_s(session, entity):
    logger.info(f"Создание сущности {entity.__tablename__}")
    session.add(entity)
    session.commit()
    session.refresh(entity)
    logger.info(f"Успешно создана сущность {entity.__tablename__}")
    return entity


def get_entities_s(session, entity_class, key=None):
    if key is not None:
        logger.info(f"Получение {entity_class.__tablename__} с ключом {key}")
        entity = session.get(entity_class, key)
        if not entity:
            raise HTTPException(
                status_code=404,
                detail=f"{entity_class.__tablename__} с ключом {key} не найден"
            )
        return entity

    logger.info(f"Получение всех сущностей {entity_class.__tablename__}")
    return session.query(entity_class).all()


def update_entity_s(session, entity_class, key, update_data):
    logger.info(f"Обновление {entity_class.__tablename__} с ключом {key}, данные: {update_data}")
    entity = session.get(entity_class, key)
    if not entity:
        raise HTTPException(
            status_code=404,
            detail=f"{entity_class.__tablename__} с ID {key} не найден"
        )

    for field, value in update_data.items():
        if value is not None:
            setattr(entity, field, value)
    session.commit()
    logger.info(f"Успешно обновлена сущность {entity_class.__tablename__} с ключом {key}")
    return entity


def delete_entity_s(session, entity_class, key):
    logger.info(f"Удаление {entity_class.__tablename__} с ключом {key}")
    entity = session.get(entity_class, key)
    if not entity:
        raise HTTPException(
            status_code=404,
            detail=f"{entity_class.__tablename__} с ID {key} не найден"
        )

    session.delete(entity)
    session.commit()
    logger.info(f"Успешно удалена сущность {entity_class.__tablename__} с ключом {key}")
    return True
