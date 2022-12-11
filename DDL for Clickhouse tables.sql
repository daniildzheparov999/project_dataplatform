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
date_of_joining date,
photo_name String
) 
ENGINE Memory;

CREATE TABLE IF NOT EXISTS clch_db.worklogs 
(
worklog_id UInt32, 
department_name String,
employee_name String,
worklog_hours String
) 
ENGINE Memory;