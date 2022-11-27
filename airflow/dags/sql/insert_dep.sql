TRUNCATE core.departments;

INSERT INTO core.departments 
SELECT 	"DepartmentId",
		"DepartmentName" 
FROM public."EmployeeApp_departments"