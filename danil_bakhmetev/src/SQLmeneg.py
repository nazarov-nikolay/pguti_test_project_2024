import sqlite3
import logging
from Parser import Parser_Employee

logging.basicConfig(level=logging.INFO)

class SQLmeneg:
    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect('employee.db')
        self.cur = self.conn.cursor()
        
    def create_db(self):
        """Creates a database with the following structure:

        E_ID = int,
        E_NAME = varchar(30),
        E_DESIGNATION = varchar(40),
        E_ADDR = varchar(100),
        E_BRANCH = varchar(15),
        E_CONT_NO = int
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
        
    def add_employee(self):
        """добовление всего что содежиться в csv используя парсер"""
        logging.info("Start adding employee")
        s = Parser_Employee(self.path)
       
       # выбираем значения из словаря и добавляем в базу
        for i in s.data:
            if i is None or i['E_ID'] is None or i['E_NAME'] is None or i['E_DESIGNATION'] is None or i['E_ADDR'] is None or i['E_BRANCH'] is None or i['E_CONT_NO'] is None:
                logging.error("Error: Some of the values in the CSV file are empty")
                raise ValueError("Error: Some of the values in the CSV file are empty")
            
            # 1. выбираем строку из базы, если есть, то обновляем, если нет, то добавляем
            self.cur.execute("""
                SELECT * FROM EMPLOYEE WHERE E_ID = ?
            """, (i['E_ID'],))
            
            # 2. если нет, то добавляем
            row = self.cur.fetchone()
            
            if row is None:
                logging.info(f"Add employee with id {i['E_ID']}")
                self.cur.execute("""
                    INSERT INTO EMPLOYEE (E_ID, E_NAME, E_DESIGNATION, E_ADDR, E_BRANCH, E_CONT_NO) 
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (i['E_ID'], i['E_NAME'], i['E_DESIGNATION'], i['E_ADDR'], i['E_BRANCH'], i['E_CONT_NO']))
                
            # 3. если есть, то обновляем
            else:
                logging.info(f"Update employee with id {i['E_ID']}")
                self.cur.execute("""
                    UPDATE EMPLOYEE SET E_NAME = ?, E_DESIGNATION = ?, E_ADDR = ?, E_BRANCH = ?, E_CONT_NO = ?
                    WHERE E_ID = ?
                """, (i['E_NAME'], i['E_DESIGNATION'], i['E_ADDR'], i['E_BRANCH'], i['E_CONT_NO'], i['E_ID']))
                
            # 4. сохраняем изменения
            self.conn.commit()
            
        logging.info("End adding employee")
            
            
s = SQLmeneg('./Employee_Details.csv')
s.create_db()
s.add_employee()