import argparse
import logging
import sys
from pathlib import Path
from typing import Optional, Dict

from app.etl.extractor import read_file
from app.etl.loader import ETLStats, validate_data, load_data
from app.etl.mappings import TABLE_MODELS, COLUMN_MAPPINGS, LINE

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s:\t%(message)s'
)
logger = logging.getLogger(__name__)


def run_etl(file_path: str, table_name: str,
            file_format: Optional[str] = None,
            column_mapping: Optional[Dict[str, str]] = None,
            validate_only: bool = False) -> ETLStats:
    logger.info(f"- Файл: {file_path}")
    logger.info(f"- Таблица: {table_name}")
    logger.info(f"- Режим: {'валидация' if validate_only else 'загрузка'}")

    df = read_file(file_path, file_format)
    logger.info(f"Прочитано строк: {len(df)}, колонок: {len(df.columns)}")
    logger.info(f"Колонки: {', '.join(df.columns)}")

    model_class = TABLE_MODELS.get(table_name.lower())
    if model_class is None:
        raise ValueError(f"Неизвестная таблица: {table_name}. "
                         f"Доступные: {', '.join(TABLE_MODELS.keys())}")

    if column_mapping is None:
        column_mapping = COLUMN_MAPPINGS.get(model_class, {})

    if validate_only:
        logger.info("Режим валидации: данные не будут загружены в БД")
        return validate_data(df, model_class, column_mapping)
    else:
        return load_data(df, model_class, column_mapping)


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='ETL для импорта данных в БД Яндекс.Такси',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    subparsers = parser.add_subparsers(dest='command', help='Доступные команды', required=True)

    import_parser = subparsers.add_parser('import', help='Импорт данных из файла в БД')

    import_parser.add_argument(
        '-f', '--file',
        type=str,
        required=True,
        metavar='PATH',
        help='Путь к файлу с данными'
    )

    import_parser.add_argument(
        '-t', '--table',
        type=str,
        required=True,
        metavar='TABLE',
        help='Уточнение названия таблицы для импорта'
    )

    import_parser.add_argument(
        '--format',
        type=str,
        choices=['csv', 'xlsx', 'xls', 'ods', 'odt'],
        metavar='FORMAT',
        help='Уточнение формата файла'
    )

    import_parser.add_argument(
        '--validate-only',
        action='store_true',
        help='Только валидация данных без загрузки в БД'
    )

    subparsers.add_parser('list-tables', help='Показать список доступных таблиц')

    return parser


def command_import(args):
    file_path = Path(args.file)
    if not file_path.exists():
        logger.error(f"Файл не найден: {args.file}")
        return 1

    logger.info(LINE)
    logger.info(f"ИМПОРТ ДАННЫХ")
    logger.info(LINE)

    try:
        stats = run_etl(
            file_path=args.file,
            table_name=args.table,
            file_format=args.format,
            validate_only=args.validate_only
        )

        print(stats)

        logger.info(LINE)
        logger.info(f"ДЕТАЛИ ОШИБОК ({len(stats.errors)} шт.)")
        logger.info(LINE)

        for i, error in enumerate(stats.errors[:50], 1):
            logger.error(f"[Ошибка {i}] Строка {error['row']}: {error['error']}")
            if error['data']:
                logger.error(f"  Данные: {error['data']}")

        if len(stats.errors) > 50:
            logger.info(f"... и еще {len(stats.errors) - 50} ошибок")

        if stats.error_count == 0:
            logger.info("Импорт завершен успешно!")
            return 0
        elif stats.success_count > 0:
            logger.warning(f"Импорт завершен с ошибками: "
                           f"{stats.success_count} успешно, {stats.error_count} ошибок")
            return 1
        else:
            logger.error("Импорт завершен с ошибками: ни одна строка не загружена")
            return 2

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        return 3


def command_list_tables():
    logger.info(LINE)
    logger.info("ДОСТУПНЫЕ ТАБЛИЦЫ ДЛЯ ИМПОРТА")
    logger.info(LINE)

    tables = {}
    for name, model in TABLE_MODELS.items():
        table_name = model.__tablename__
        if table_name not in tables:
            tables[table_name] = []
        tables[table_name].append(name)

    for i, (table_name, aliases) in enumerate(sorted(tables.items()), 1):
        logger.info(f"{i:2}. {table_name:20} (или: {', '.join(aliases)})")

    logger.info(LINE)
    return 0


def main():
    parser = create_parser()
    args = parser.parse_args()

    if args.command == 'import':
        exit_code = command_import(args)
    elif args.command == 'list-tables':
        exit_code = command_list_tables()
    else:
        parser.print_help()
        exit_code = 1

    sys.exit(exit_code)


if __name__ == '__main__':
    main()
