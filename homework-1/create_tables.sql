-- SQL-команды для создания таблиц


CREATE TABLE EMPLOYEES (EMPLOYEE_ID int PRIMARY KEY,
FIRST_NAME VARCHAR(20) UNIQUE NOT NULL,
LAST_NAME VARCHAR(20),
TITLE VARCHAR(40),
BIRTH_DATE date, NOTES text);


CREATE TABLE CUSTOMERS (CUSTOMER_ID char(5) PRIMARY KEY,
COMPANY_NAME VARCHAR(40) NOT NULL,
CONTACT_NAME VARCHAR(40));


CREATE TABLE ORDERS (ORDER_ID int PRIMARY KEY,
CUSTOMER_ID char(5) REFERENCES CUSTOMERS(CUSTOMER_ID) NOT NULL,
EMPLOYEE_ID int REFERENCES EMPLOYEES(EMPLOYEE_ID) NOT NULL,
ORDER_DATE date, SHIP_CITY VARCHAR(20));
