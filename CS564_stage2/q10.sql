SELECT 
	e.employee_id
FROM 
	employees e
LEFT JOIN 
	dependents d ON d.employee_id = e.employee_id
WHERE 
	d.employee_id IS NULL;
