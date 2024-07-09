import os
import pandas as pd
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def sql_query(engine, output_directory, sql_queries_directory):  # написание SQL запроса к базе
    queries_file = []
    for i in os.listdir(sql_queries_directory):
        if i.endswith('.sql'):
            queries_file.append(i)
    logger.info(f'Доступные SQL-запросы в директории: {sql_queries_directory}: {queries_file}')

    queries_confirmation = input(f'Подтвердите выполнение запросов [+ / -]: ')
    if queries_confirmation == '+':
        try:
            for query_file in queries_file:
                query_file_path = os.path.join(sql_queries_directory, query_file)  # путь к SQL запросам
                query_name = query_file.split('.')[0]  # название файла результата

                with open(query_file_path, 'r') as file:
                    sql_query = file.read()

                try:
                    df_result = pd.read_sql_query(sql_query, engine)  # выполнение SQL-запроса и сохранение результатов в DataFrame

                    output_path = fr'{output_directory}/{query_name}.csv'  # путь для выгрузки результатов
                    df_result.to_csv(output_path, index=True)
                    logger.info(f'Результат запроса в {query_name}.csv сохранен по пути: {output_path}')

                except Exception as e:
                    if 'This result object does not return rows. It has been closed automatically.' in str(e):
                        with engine.connect() as conn:
                            conn.execute(text(sql_query))
                            conn.commit() # завершаем транзакцию и сохраняем изменения в бд
                        logger.info(f'Запрос {query_name} выполнен успешно')
                    else:
                        logger.error(f'Ошибка при выполнении SQL-запроса: {e}')

        except Exception as e:
            logger.error(f'Ошибка при выполнении SQL-запроса или сохранении файла: {e}')