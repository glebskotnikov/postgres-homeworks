"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv

import psycopg2

conn = psycopg2.connect(host='localhost', database='north', user='postgres', password='skypro2023')

try:
    with conn:
        with conn.cursor() as cur:
            with open('../homework-1/north_data/employees_data.csv', encoding='CP1251') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for item in reader:
                    cur.execute('INSERT INTO employees VALUES(%s, %s, %s, %s, %s, %s)',
                                    (item['employee_id'], item['first_name'], item['last_name'],
                                     item['title'], item['birth_date'], item['notes']))
            with open('../homework-1/north_data/customers_data.csv', encoding='CP1251') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for item in reader:
                    cur.execute('INSERT INTO customers VALUES(%s, %s, %s)',
                                    (item['customer_id'], item['company_name'], item['contact_name']))
            with open('../homework-1/north_data/orders_data.csv', encoding='CP1251') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for item in reader:
                    cur.execute('INSERT INTO orders VALUES(%s, %s, %s, %s, %s)',
                                    (item['order_id'], item['customer_id'], item['employee_id'],
                                     item['order_date'], item['ship_city']))
finally:
    conn.close()