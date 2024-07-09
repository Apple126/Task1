import logging


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('app.log'),  # записываем логи в файл app.log
            logging.StreamHandler()  # выводим логи на консоль
        ]
    )
