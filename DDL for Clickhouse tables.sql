CREATE DATABASE clch_db;

CREATE TABLE IF NOT EXISTS clch_db.departments 
(
department_id UInt32, 
department_name String
) 
ENGINE Memory;

CREATE TABLE IF NOT EXISTS clch_db.employees 
(
employee_id UInt32, 
employee_name String,
department_name String,
date_of_joining date
) 
ENGINE Memory;

CREATE TABLE IF NOT EXISTS clch_db.worklogs 
(
worklog_id UInt32, 
employee_id String,
worklog_date date,
worklog_hours String
) 
ENGINE Memory;

CREATE TABLE IF NOT EXISTS clch_db.worklogs_dm 
ENGINE = Memory
AS 
SELECT w.worklog_id,
emp.employee_name, 
emp.department_name,
emp.date_of_joining,
w.worklog_date,
w.worklog_hours 
FROM clch_db.worklogs w 
LEFT JOIN clch_db.employees emp
ON w.employee_id = toString(emp.employee_id);