-- Напишите запросы, которые выводят следующую информацию:
-- 1. Название компании заказчика (company_name из табл. customers) и ФИО сотрудника, работающего над заказом этой компании (см таблицу employees),
-- когда и заказчик и сотрудник зарегистрированы в городе London, а доставку заказа ведет компания United Package (company_name в табл shippers)
select customers.company_name,
concat(employees.first_name, ' ', employees.last_name) as full_name
from orders
inner join customers using(customer_id)
inner join employees using(employee_id)
inner join shippers on orders.ship_via = shippers.shipper_id
where customers.city = 'London' and
employees.city = 'London' and
shippers.company_name = 'United Package';

-- 2. Наименование продукта, количество товара (product_name и units_in_stock в табл products),
-- имя поставщика и его телефон (contact_name и phone в табл suppliers) для таких продуктов,
-- которые не сняты с продажи (поле discontinued) и которых меньше 25 и которые в категориях Dairy Products и Condiments.
-- Отсортировать результат по возрастанию количества оставшегося товара.
select products.product_name, products.units_in_stock,
suppliers.contact_name, suppliers.phone
from products inner join suppliers using(supplier_id)
inner join categories using(category_id)
where products.discontinued = 0 and
products.units_in_stock < 25 and
categories.category_name in ('Dairy Products', 'Condiments')
order by units_in_stock;

-- 3. Список компаний заказчиков (company_name из табл customers), не сделавших ни одного заказа
select customers.company_name
from customers
where customer_id not in (select customer_id from orders);

-- 4. уникальные названия продуктов, которых заказано ровно 10 единиц (количество заказанных единиц см в колонке quantity табл order_details)
-- Этот запрос написать именно с использованием подзапроса.
select products.product_name
from order_details inner join products using(product_id)
where order_details.quantity = 10 and
order_id in (select order_id from orders);