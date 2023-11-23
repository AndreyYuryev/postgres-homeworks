-- 1. Создать таблицу student с полями student_id serial, first_name varchar, last_name varchar, birthday date, phone varchar
CREATE TABLE STUDENT
(
STUDENT_ID serial PRIMARY KEY,
FIRST_NAME varchar,
LAST_NAME varchar,
BIRTHDAY date,
PHONE varchar
) ;

-- 2. Добавить в таблицу student колонку middle_name varchar
ALTER TABLE STUDENT ADD COLUMN MIDDLE_NAME varchar;

-- 3. Удалить колонку middle_name
ALTER TABLE STUDENT DROP COLUMN MIDDLE_NAME;

-- 4. Переименовать колонку birthday в birth_date
ALTER TABLE STUDENT RENAME COLUMN BIRTHDAY TO BIRTH_DATE;

-- 5. Изменить тип данных колонки phone на varchar(32)
ALTER TABLE STUDENT ALTER COLUMN PHONE SET DATA TYPE VARCHAR(32);

-- 6. Вставить три любых записи с автогенерацией идентификатора
INSERT INTO STUDENT (FIRST_NAME,LAST_NAME, BIRTH_DATE, PHONE)
VALUES ('Ivan', 'Ivanov', '20.12.2000', '4954573224'),
('Alex', 'Petrov', '21.11.2000', '4954563214'),
('Semen', 'Sidorov', '10.04.2000', '4958563284');

-- 7. Удалить все данные из таблицы со сбросом идентификатор в исходное состояние
TRUNCATE TABLE STUDENT RESTART IDENTITY;