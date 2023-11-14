"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
from dotenv import load_dotenv
import os
from csv import DictReader, reader


def env():
    """ Загрузка переменных окружения """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.split(current_dir)[0]
    filepath = os.path.join(root_dir, '.env')
    if os.path.exists(filepath):
        load_dotenv(filepath)
        return os.getenv('USER'), os.getenv('PASSWORD')


def load_csv(filename='', fields=[]):
    """ Загрузка данных из файла """
    table = list()
    line = dict()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.split(current_dir)[0]
    filepath = os.path.join(root_dir, os.path.normpath('homework-1'), os.path.normpath('north_data'), filename)
    if os.path.isfile(filepath):
        with open(file=filepath, mode='r', encoding='utf-8') as file:
            lines = reader(file, delimiter=",")
            index = 0
            for row in lines:
                if index == 0:
                    index += 1
                    continue
                for itm in zip(fields, row):
                    line[itm[0]] = itm[1]
                table.append(line.copy())
                line.clear()
    return table


def main():
    """ Запись загруженных данных в таблицы """
    data_customers = load_csv(filename='customers_data.csv', fields=["customer_id", "company_name", "contact_name"])
    data_employees = load_csv(filename='employees_data.csv',
                              fields=["employee_id", "first_name", "last_name", "title", "birth_date", "notes"])
    data_orders = load_csv(filename='orders_data.csv',
                           fields=["order_id", "customer_id", "employee_id", "order_date", "ship_city"])
    credential = env()
    conn = psycopg2.connect(
        host='localhost',
        database='north',
        user=credential[0],
        password=credential[1]
    )
    try:
        with conn:
            with conn.cursor() as curr:
                sql_customers = 'INSERT INTO customers VALUES ( %s, %s, %s )'
                for item in data_customers:
                    curr.execute(sql_customers, (item['customer_id'], item['company_name'], item['contact_name']))

                sql_employees = 'INSERT INTO employees VALUES ( %s, %s, %s, %s, %s, %s )'
                for item in data_employees:
                    curr.execute(sql_employees, (item['employee_id'], item['first_name'], item['last_name'],
                                                 item['title'], item['birth_date'], item['notes']))

                sql_orders = 'INSERT INTO orders VALUES ( %s, %s, %s, %s, %s )'
                for item in data_orders:
                    curr.execute(sql_orders, (item['order_id'], item['customer_id'], item['employee_id'],
                                              item['order_date'], item['ship_city']))
    finally:
        conn.close()


if __name__ == '__main__':
    main()
