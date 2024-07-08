import os
import pandas as pd
import logging


logger = logging.getLogger(__name__)

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
    upload_confirmation = input(f'Требуется ли загрузка файлов в базу {db_params["database"]}? [+ / -]: ')
    if upload_confirmation == '+':
        which_files = input('Загрузить все указанные файлы [1] или выбрать определенные [2]? [1 / 2]: ')
        if which_files == '1':
            for file in json_files:
                file_path = os.path.join(results_directory, file) # путь для выгрузки результата SQL запроса файлом
                table_name = file.split('.')[0] # название таблицы SQL = название файла json
                df = pd.read_json(f'{file_path}')
                df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
                logger.info(f'Загрузка данных из {file} в таблицу {table_name}')
        elif which_files == '2':
            files_input = input('Введите необходимые к загрузке файлы через пробел [например students.json]: ')
            specific_files = files_input.split()
            for file in specific_files:
                file_path = os.path.join(results_directory, file)
                table_name = file.split('.')[0]
                df = pd.read_json(f'{file_path}')
                df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
                logger.info(f'Загрузка данных из {file} в таблицу {table_name}')
        else:
            logger.info('Загрузка отменена')