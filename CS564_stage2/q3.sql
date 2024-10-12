SELECT 
	d.department_name, 
	AVG(j.max_salary) AS average_max_salary
	
FROM
	departments d
JOIN
	employees e ON d.department_id = e.department_id
JOIN 
	jobs j ON e.job_id = j.job_id  
GROUP BY
	d.department_name
HAVING AVG(j.max_salary) >8000	
