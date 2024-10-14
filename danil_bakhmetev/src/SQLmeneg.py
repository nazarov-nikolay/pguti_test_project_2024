import sqlite3
import logging
from typing import List, Dict, Any
from Parser import Parser_Employee


class SQLmeneg:
    """
    Класс для работы с базой данных.
    """
    def __init__(self, path: str):
        """
        Инициализация класса.
        
        :param path: Путь к файлу базы данных.
        """
        self.path = path
        self.conn = sqlite3.connect(path)
        self.cur = self.conn.cursor()
    
    def create_db(self) -> None:
        """
        Создание базы данных.
        """
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS EMPLOYEE (
                E_ID INTEGER PRIMARY KEY,
                E_NAME VARCHAR(30),
                E_DESIGNATION VARCHAR(40),
                E_ADDR VARCHAR(100),
                E_BRANCH VARCHAR(15),
                E_CONT_NO INTEGER
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
        Check if a row is valid.

        A row is valid if it has the correct types and lengths for each column.

        :param row: A dictionary with employee information.
        :return: True if the row is valid, False otherwise.
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
            SELECT * FROM EMPLOYEE WHERE E_ID = ?
        """
        self.cur.execute(query, (row['E_ID'],))
        employee = self.cur.fetchone()
        
        if employee is None:
            # добавляем нового сотрудника
            query = """
                INSERT INTO EMPLOYEE (E_ID, E_NAME, E_DESIGNATION, E_ADDR, E_BRANCH, E_CONT_NO) 
                VALUES (?, ?, ?, ?, ?, ?)
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
                UPDATE EMPLOYEE SET E_NAME = ?, E_DESIGNATION = ?, E_ADDR = ?, E_BRANCH = ?, E_CONT_NO = ?
                WHERE E_ID = ?
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
    s = SQLmeneg('./Employee_Details.db')
    s.create_db()
    p = Parser_Employee('./Employee_Details.csv')
    s.add_employees(p.data)
