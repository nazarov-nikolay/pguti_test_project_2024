import psycopg2
import logging
from psycopg2 import sql
from typing import List, Dict, Any
from Parser import Parser_Employee


class SQLmeneg:
    """
    Класс для работы с базой данных PostgreSQL.
    """
    def __init__(self, db_name: str, user: str, password: str, host: str = 'localhost', port: str = '5432'):
        """
        Инициализация класса.

        :param db_name: Имя базы данных.
        :param user: Пользователь базы данных.
        :param password: Пароль для доступа к базе данных.
        :param host: Хост базы данных, по умолчанию 'localhost'.
        :param port: Порт базы данных, по умолчанию '5432'.
        """
        self.conn = psycopg2.connect(
            dbname='postgres',  # Подключаемся к базе данных postgres для создания новой базы
            user=user,
            password=password,
            host=host,
            port=port,
            options='-c client_encoding=UTF8'
        )
        self.cur = self.conn.cursor()
        self.create_db(db_name)

        # После создания базы данных, повторное подключение к новой базе
        self.conn.close()
        self.conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port,
            options='-c client_encoding=UTF8'
        )
        self.cur = self.conn.cursor()

    def create_db(self, db_name: str) -> None:
        """
        Создание базы данных, если она не существует.

        :param db_name: Имя базы данных для создания.
        """
        try:
            self.cur.execute(sql.SQL("SELECT 1 FROM pg_database WHERE datname = %s"), [db_name])
            exists = self.cur.fetchone()
            if not exists:
                self.cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
                logging.info(f"База данных '{db_name}' была успешно создана.")
            else:
                logging.info(f"База данных '{db_name}' уже существует.")
            self.conn.commit()
        except Exception as e:
            logging.error(f"Ошибка при создании базы данных: {e}")
            self.conn.rollback()

    def create_table(self) -> None:
        """
        Создание таблицы в базе данных.
        """
        self.cur.execute(""" 
            CREATE TABLE IF NOT EXISTS EMPLOYEE (
                E_ID SERIAL PRIMARY KEY,
                E_NAME VARCHAR(30),
                E_DESIGNATION VARCHAR(40),
                E_ADDR VARCHAR(100),
                E_BRANCH VARCHAR(15),
                E_CONT_NO BIGINT
            )
        """)
        self.conn.commit()

    def add_employees(self, data: List[Dict[str, Any]]) -> None:
        """
        Добавление сотрудников в базу данных.

        :param data: Список словарей с информацией о сотрудниках.
        """
        if not data:
            logging.warning("List of employees is empty")
            return

        logging.info("Start adding employees")
        for i in data:
            if not self._is_valid_row(i):
                continue
            self._add_or_update(i)
        logging.info("End adding employees")

    def _is_valid_row(self, row: Dict[str, Any]) -> bool:
        """
        Проверка корректности данных сотрудника.

        :param row: Словарь с информацией о сотруднике.
        :return: True если данные корректны, False в противном случае.
        """
        columns = [
            ('E_ID', int),
            ('E_NAME', str, 30),
            ('E_DESIGNATION', str, 40),
            ('E_ADDR', str, 100),
            ('E_BRANCH', str, 15),
            ('E_CONT_NO', int),
        ]
        for column, expected_type, *expected_lengths in columns:
            value = row[column]
            if not isinstance(value, expected_type):
                return False
            if expected_lengths and len(value) > expected_lengths[0]:
                return False
        return True

    def _add_or_update(self, row: Dict[str, Any]) -> None:
        """
        Добавляет или обновляет сотрудника в базе данных.

        :param row: Словарь с информацией о сотруднике.
        """
        query = """
            SELECT * FROM EMPLOYEE WHERE E_ID = %s
        """
        self.cur.execute(query, (row['E_ID'],))
        employee = self.cur.fetchone()

        if employee is None:
            # добавляем нового сотрудника
            query = """
                INSERT INTO EMPLOYEE (E_ID, E_NAME, E_DESIGNATION, E_ADDR, E_BRANCH, E_CONT_NO) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            self.cur.execute(query, (
                row['E_ID'],
                row['E_NAME'],
                row['E_DESIGNATION'],
                row['E_ADDR'],
                row['E_BRANCH'],
                row['E_CONT_NO'],
            ))
        else:
            # обновляем информацию о существующем сотруднике
            query = """
                UPDATE EMPLOYEE SET E_NAME = %s, E_DESIGNATION = %s, E_ADDR = %s, E_BRANCH = %s, E_CONT_NO = %s
                WHERE E_ID = %s
            """
            self.cur.execute(query, (
                row['E_NAME'],
                row['E_DESIGNATION'],
                row['E_ADDR'],
                row['E_BRANCH'],
                row['E_CONT_NO'],
                row['E_ID'],
            ))

        # сохраняем изменения
        self.conn.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    s = SQLmeneg(db_name='Employee_Details', user='kukuvs', password='4269')
    s.create_table()  # Создаем таблицу после создания базы данных
    p = Parser_Employee('./Employee_Details.csv')
    s.add_employees(p.data)
