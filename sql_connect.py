import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

db_params = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
}

engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}') # подключение к Postgre SQL

# Путь к директории с JSON файлами внутри контейнера
json_directory = r'/app/BigData'

# определяем json файлы в указанной папке
json_files = []
for i in os.listdir(json_directory):
    if i.endswith('.json'):
        json_files.append(i)
print(f'Выбранные для загрузки в базу файлы: {json_files}')

# Чтение параметров загрузки из переменных окружения
need_to_upload = os.getenv('UPLOAD_FILES', '+')
if need_to_upload == '+':
    for file in json_files:
        file_path = os.path.join(json_directory, file)
        table_name = file.split('.')[0]
        df = pd.read_json(file_path)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f'Загрузка данных из {file} в таблицу {table_name}')
else:
    print('Загрузка отменена')

# Чтение SQL-запроса и параметров сохранения из переменных окружения
sql_query = os.getenv('SQL_QUERY')
if sql_query:
    file_format = os.getenv('FILE_FORMAT', 'json').strip().lower()
    file_name = os.getenv('FILE_NAME', 'output')

    output_path = fr'/app/BigData/Results/{file_name}.{file_format}'

    # Выполнение SQL-запроса и сохранение результатов в DataFrame
    df_result = pd.read_sql_query(sql_query, engine)

    # Сохранение результата в папку, с выбранным форматом
    if file_format == 'json':
        df_result.to_json(output_path, orient='records')
    elif file_format == 'xml':
        df_result.to_xml(output_path)
    elif file_format == 'csv':
        df_result.to_csv(output_path, index=True)

    print(f'Результат запроса в файле {file_name}.{file_format} сохранен по пути: {output_path}')
else:
    print('SQL-запрос не был указан')