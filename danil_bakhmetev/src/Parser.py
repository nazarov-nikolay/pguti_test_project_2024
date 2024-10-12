import csv


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
                    self.data.append(tuple(row.values()))
        except IOError as e:
            # если возникла ошибка при чтении файла,
            # то поднимаем исключение IOError
            raise IOError(f"Ошибка при чтении {path}: {e}") from e


# создаем объект класса
s = Parser_Employee('src/Employee_Details.csv')

# печатаем результат
print(s.data)

