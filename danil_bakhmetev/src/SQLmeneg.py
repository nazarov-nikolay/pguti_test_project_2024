import psycopg2

# Установите соответствующие значения для вашего случая
dbname = 'postgres'
user = 'postgres'
password = '42695'
host = 'localhost'
port = '5432'

conn = None
cur = None

try:
    # Подключение к базе данных
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # Создание курсора для выполнения SQL-запросов
    cur = conn.cursor()

    # Выполнение простого SQL-запроса
    cur.execute('SELECT version()')

    # Получение результата запроса
    db_version = cur.fetchone()

    print(f'Connected to PostgreSQL version: {db_version}')

except Exception as e:
    print(f'Error connecting to PostgreSQL: {e}')

finally:
    # Закрытие курсора и соединения
    if cur:
        cur.close()
    if conn:
        conn.close()