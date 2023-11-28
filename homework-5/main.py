import json
import os
import psycopg2

from config import config


def main():
    script_file = 'fill_db.sql'
    json_file = 'suppliers.json'
    db_name = 'sky'

    params = config()
    conn = None

    create_database(params, db_name)
    print(f"БД {db_name} успешно создана")

    params.update({'dbname': db_name})
    try:
        with psycopg2.connect(**params) as conn:
            with conn.cursor() as cur:
                execute_sql_script(cur, script_file)
                print(f"БД {db_name} успешно заполнена")

                create_suppliers_table(cur)
                print("Таблица suppliers успешно создана")

                suppliers = get_suppliers_data(json_file)
                insert_suppliers_data(cur, suppliers)
                print("Данные в suppliers успешно добавлены")

                add_foreign_keys(cur, json_file)
                print(f"FOREIGN KEY успешно добавлены")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_database(params, db_name) -> None:
    """Создает новую базу данных."""
    pass


def execute_sql_script(cur, script_file) -> None:
    """Выполняет скрипт из файла для заполнения БД данными."""
    with open(script_file, 'r') as sql:
        line = sql.read()
        cur.execute(line)


def create_suppliers_table(cur) -> None:
    """Создает таблицу suppliers."""
    sql = '''CREATE TABLE suppliers (
    supplier_id smallint NOT NULL PRIMARY KEY,
    company_name character varying(40) NOT NULL,
    contact_name character varying(30),
    contact_title character varying(30),
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postal_code character varying(10),
    country character varying(15),
    phone character varying(24),
    fax character varying(24),
    homepage text
); '''
    cur.execute(sql)
    sql = '''ALTER TABLE products ADD COLUMN supplier_id smallint;'''
    cur.execute(sql)
    sql = '''ALTER TABLE products ADD CONSTRAINT fk_products_suppliers FOREIGN KEY(supplier_id) REFERENCES suppliers(supplier_id);'''
    cur.execute(sql)


def get_suppliers_data(json_file: str) -> list[dict]:
    """Извлекает данные о поставщиках из JSON-файла и возвращает список словарей с соответствующей информацией."""
    if os.path.isfile(json_file):
        with open(file=json_file, mode='r', encoding='utf-8') as file:
            data = json.load(file)
    return data


def insert_suppliers_data(cur, suppliers: list[dict]) -> None:
    """Добавляет данные из suppliers в таблицу suppliers."""
    index = 0
    sql_employees = 'INSERT INTO suppliers VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s );'
    for item in suppliers:
        index += 1
        company_name = item.get('company_name')
        contact = item.get('contact')
        if contact is not None:
            contact_name = contact.split(',')[0]
            contact_title = contact.split(',')[1]
        address_row = item.get('address')
        if address_row is not None:
            address_row_l = address_row.split(';')
            address = address_row_l[4]
            city = address_row_l[3]
            region = address_row_l[1] if address_row_l[1] == '' else None
            postal_code = address_row_l[2]
            country = address_row_l[0]
        phone = item.get('phone')
        fax = item.get('fax')
        homepage = item.get('homepage')
        cur.execute(sql_employees, (
            index, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax,
            homepage))


def add_foreign_keys(cur, json_file) -> None:
    """Добавляет foreign key со ссылкой на supplier_id в таблицу products."""
    if os.path.isfile(json_file):
        with open(file=json_file, mode='r', encoding='utf-8') as file:
            data = json.load(file)
        index = 0
        sql_employees = 'UPDATE products SET supplier_id=( %s ) WHERE product_name =( %s );'
        for item in data:
            index += 1
            for product in item['products']:
                cur.execute(sql_employees, (index, product))


if __name__ == '__main__':
    main()
