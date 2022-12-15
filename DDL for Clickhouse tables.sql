SELECT *
FROM clch_db.worklogs w ;

INSERT INTO clch_db.departments 
VALUES 
(1, 'HR');

CREATE DATABASE clch_db;

CREATE TABLE IF NOT EXISTS clch_db.departments 
(
department_id UInt32, 
department_name String
) 
ENGINE Log;

CREATE TABLE IF NOT EXISTS clch_db.employees 
(
employee_id UInt32, 
employee_name String,
department_name String,
date_of_joining date
) 
ENGINE Log;

CREATE TABLE IF NOT EXISTS clch_db.worklogs 
(
worklog_id UInt32, 
employee_id String,
worklog_date date,
worklog_hours String
) 
ENGINE Log;

CREATE TABLE IF NOT EXISTS clch_db.worklogs_dm 
(
`worklog_id` UInt32,
`employee_name` String,
`department_name` String,
`date_of_joining` Date,
`worklog_date` Date,
`worklog_hours` Int32
)
ENGINE = Log;

CREATE TABLE IF NOT EXISTS clch_db.worklogs_dm 
ENGINE = Log
AS 
SELECT w.worklog_id,
emp.employee_name, 
emp.department_name,
emp.date_of_joining,
w.worklog_date,
toInt32(w.worklog_hours) AS worklog_hours
FROM clch_db.worklogs w 
LEFT JOIN clch_db.employees emp
ON w.employee_id = toString(emp.employee_id);