import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def connection_to_database(db_params):
    try:
        engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        engine.connect()
        logger.info('Успешное подключение к базе данных')
        return engine
    except Exception as e:
        logger.error(f'Ошибка при подключении к базе данных: {e}')
        raise
# функция для подключения к базе даннных (подключаемся к базе с исп. параметров из словаря db_params)