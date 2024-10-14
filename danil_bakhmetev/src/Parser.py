import csv
import logging

class Parser_Employee:
    """
    Класс для парсинга CSV файлов
    """
    def __init__(self, path):
        self.data = []
        self.parse_csv(path)

    def parse_csv(self, path: str) -> None:
        """Читает CSV файл в память

        Аргументы:
            path (str): Путь к файлу

        Исключения:
            ValueError: Если path пустой
            IOError: Если происходит ошибка при чтении файла
        """
        # Проверка на пустоту пути
        if not path:
            raise ValueError("Путь не может быть пустым")

        try:
            # Открываем файл для чтения
            with open(path, "r", newline="") as file:
                # создаем объект reader для чтения CSV
                reader = csv.DictReader(file)
                # Читаем файл построчно и добавляем
                # каждую строку в список data
                for row in reader:
                    if self.check_row(row):
                        self.data.append(dict(row))
                    else:
                        logging.error(f"Строка {row} не соответствует структуре")
        except IOError as e:
            # если возникла ошибка при чтении файла,
            # то поднимаем исключение IOError
            raise IOError(f"Ошибка при чтении {path}: {e}") from e

    def check_row(self, row: dict) -> bool:
        """Проверяет соответствие структуры данных в строке

        Аргументы:
            row (dict): Словарь со строкой

        Возвращает:
            bool: True если соответствует, False если нет
        """
        try:
            # Преобразуем строки в нужные типы данных
            row["E_ID"] = int(row["E_ID"])
            row["E_CONT_NO"] = int(row["E_CONT_NO"])
        except ValueError as e:
            logging.error(f"Ошибка преобразования типов: {e}")
            return False

        # проверяем соответствие типов
        if not isinstance(row["E_ID"], int):
            logging.error(f"Ошибка: E_ID должен быть int, а не {type(row['E_ID']).__name__}")
            return False
        if not isinstance(row["E_NAME"], str) or len(row["E_NAME"]) > 30:
            logging.error(f"Ошибка: E_NAME должен быть str длиной не более 30, а не {type(row['E_NAME']).__name__} длиной {len(row['E_NAME'])}")
            return False
        if not isinstance(row["E_DESIGNATION"], str) or len(row["E_DESIGNATION"]) > 40:
            logging.error(f"Ошибка: E_DESIGNATION должен быть str длиной не более 40, а не {type(row['E_DESIGNATION']).__name__} длиной {len(row['E_DESIGNATION'])}")
            return False
        if not isinstance(row["E_ADDR"], str) or len(row["E_ADDR"]) > 100:
            logging.error(f"Ошибка: E_ADDR должен быть str длиной не более 100, а не {type(row['E_ADDR']).__name__} длиной {len(row['E_ADDR'])}")
            return False
        if not isinstance(row["E_BRANCH"], str) or len(row["E_BRANCH"]) > 15:
            logging.error(f"Ошибка: E_BRANCH должен быть str длиной не более 15, а не {type(row['E_BRANCH']).__name__} длиной {len(row['E_BRANCH'])}")
            return False
        if not isinstance(row["E_CONT_NO"], int):
            logging.error(f"Ошибка: E_CONT_NO должен быть int, а не {type(row['E_CONT_NO']).__name__}")
            return False

        return True
