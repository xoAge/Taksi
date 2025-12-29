import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def read_file(file_path: str, file_format: str = None) -> pd.DataFrame:
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {file_path}")

    if file_format is None:
        file_format = path.suffix.lower().lstrip('.')

    logger.info(f"Чтение файла {file_path} (формат: {file_format})")

    if file_format == 'csv':
        return _read_csv(file_path)
    elif file_format in ['xlsx', 'xls']:
        return _read_excel(file_path, file_format)
    elif file_format in ['ods', 'odt']:
        return _read_ods(file_path)
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {file_format}")


def _read_csv(file_path: str) -> pd.DataFrame:
    encodings = ['utf-8', 'cp1251', 'latin1']
    separators = [',', ';', '\t']

    for encoding in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(file_path, encoding=encoding, sep=sep)
                if len(df.columns) > 1:
                    logger.info(f"CSV прочитан (encoding={encoding}, sep='{sep}')")
                    return df
            except Exception:
                continue

    raise ValueError("Не удалось определить формат CSV файла")


def _read_excel(file_path: str, file_format: str) -> pd.DataFrame:
    engine = 'openpyxl' if file_format == 'xlsx' else 'xlrd'
    df = pd.read_excel(file_path, engine=engine)
    logger.info(f"Excel файл прочитан")
    return df


def _read_ods(file_path: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, engine='odf')
    logger.info(f"ODS/ODT файл прочитан")
    return df
