-- Напишите запросы, которые выводят следующую информацию:
-- 1. "имя контакта" и "город" (contact_name, city) из таблицы customers (только эти две колонки)
SELECT CONTACT_NAME, CITY
FROM CUSTOMERS

-- 2. идентификатор заказа и разницу между датами формирования (order_date) заказа и его отгрузкой (shipped_date) из таблицы orders
SELECT ORDER_ID, SHIPPED_DATE - ORDER_DATE AS DIFFERENCE
FROM ORDERS

-- 3. все города без повторов, в которых зарегистрированы заказчики (customers)
SELECT DISTINCT CITY
FROM CUSTOMERS

-- 4. количество заказов (таблица orders)
SELECT COUNT(*)
FROM ORDERS

-- 5. количество стран, в которые отгружался товар (таблица orders, колонка ship_country)
SELECT COUNT(DISTINCT SHIP_COUNTRY)
FROM ORDERS
