import os
from dotenv import load_dotenv
import logging

from db_connection import connection_to_database
from uploads import load_json_files, upload_json_files
from queries import sql_query
from logg_settings import setup_logging


def main():
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        logger.info('Загрузка переменных для подключения к базе данных')
        load_dotenv()

        db_params = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
        }

        source_directory = r'/home/user/PycharmProjects/Test_GitFlow/BigData' # директория с загрузочными json файлами
        output_directory = r'/home/user/PycharmProjects/Test_GitFlow/BigData/Results' # директория выгрузки результатов SQL-запроса
        sql_queries_directory = r'/home/user/PycharmProjects/Test_GitFlow/BigData/SQL_queries' # директория c SQL-запросами (файлы .sql)

        engine = connection_to_database(db_params) # подключение к базе данных
        json_files_list = load_json_files(source_directory) # получаем список файлов json
        upload_json_files(json_files_list, source_directory, db_params, engine)
        sql_query(engine, output_directory, sql_queries_directory)
    except Exception as e:
        logger.info(f'Ошибка во время выполнения скрипта: {e}')
    finally:
        logger.info('Скрипт завершен')

if __name__ == "__main__":
    main()