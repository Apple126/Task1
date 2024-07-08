import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

# Настройка логгирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'), # записываем логи в файл app.log
        logging.StreamHandler() # выводим логи на консоль
    ]
)
logger = logging.getLogger(__name__)

# Функция для подключения к базе даннных (подключаемся к базе с исп. параметров из словаря db_params)
def connection_to_database(db_params):
    try:
        engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        engine.connect()
        logger.info('Успешное подключение к базе данных')
        return engine
    except Exception as e:
        logger.error(f'Ошибка при подключении к базе данных: {e}')
        raise

# определяем json файлы в указанной папке
def load_json_files(source_directory):
    logger.info(f'Указанная директория для JSON файлов: {source_directory}')
    files = []
    for i in os.listdir(source_directory):
        if i.endswith('.json'):
            files.append(i)
    logger.info(f'Выбранные для загрузки в базу файлы: {files}')
    return files

def upload_json_files(json_files, results_directory, db_params, engine):
    upload_confirmation = input(f'Требуется ли загрузка указанных файлов в базу {db_params["database"]}? [+ или -]: ')
    if upload_confirmation == '+':
        for file in json_files:
            file_path = os.path.join(results_directory, file) # путь для выгрузки результата SQL запроса файлом
            table_name = file.split('.')[0] # название таблицы SQL = название файла json
            df = pd.read_json(f'{file_path}')
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            logger.info(f'Загрузка данных из {file} в таблицу {table_name}')
    else:
        logger.info('Загрузка отменена')

def sql_query(engine, output_directory, sql_queries_directory):  # написание SQL запроса к базе
    queries_file = []
    for i in os.listdir(sql_queries_directory):
        if i.endswith('.sql'):
            queries_file.append(i)
    logger.info(f'Доступные SQL-запросы в директории: {sql_queries_directory}: {queries_file}')

    queries_confirmation = input(f'Подтвердите выполнение запросов: [+ или -]')
    if queries_confirmation == '+':
        try:
            for query_file in queries_file:
                query_file_path = os.path.join(sql_queries_directory, query_file)  # путь к SQL запросам
                query_name = query_file.split('.')[0]  # название файла результата

                with open(query_file_path, 'r') as file:
                    sql_query = file.read()

                df_result = pd.read_sql_query(sql_query, engine)  # выполнение SQL-запроса и сохранение результатов в DataFrame

                output_path = fr'{output_directory}/{query_name}.csv'  # путь для выгрузки результатов
                df_result.to_csv(output_path, index=True)
                logger.info(f'Результат запроса в {query_name}.csv сохранен по пути: {output_path}')
        except Exception as e:
            logger.error(f'Ошибка при выполнении SQL-запроса или сохранении файла: {e}')

def main():
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

        source_directory = r'/home/user/Desktop/BigData' # директория с загрузочными json файлами
        output_directory = r'/home/user/Desktop/BigData/Results' # директория выгрузки результатов SQL-запроса
        sql_queries_directory = r'/home/user/Desktop/BigData/SQL_queries' # директория c SQL-запросами (файлы .sql)

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