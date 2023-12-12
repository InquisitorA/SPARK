val empDF = spark.read.option("header", "true").csv("hdfs:///tmp/Aryan/Ass7/emp.csv")

empDF.show()

val depDF = spark.read.option("header", "true").csv("hdfs:///tmp/Aryan/Ass7/dep.csv")

depDF.show()

empDF.createOrReplaceTempView("emp")

depDF.createOrReplaceTempView("dep")

val kpiDF = spark.sql("""
SELECT emp_id, 
        emp_name,  
        emp_age, 
        dep_id, 
        dep_name FROM (
            SELECT emp_id, emp_name, emp_age, dep_id, dep_name, salary, 
            ROW_NUMBER() OVER (PARTITION BY dep_id ORDER BY salary DESC) as rank
            FROM (  
                SELECT 
                    emp_id,
                    CONCAT(emp_fname, ' ', emp_lname) AS emp_name,
                    emp_Age AS emp_age,
                    emp_depID AS dep_id,
                    depname AS dep_name,
    emp.emp_salary as salary 
FROM
    emp
JOIN
    dep ON emp.emp_depID = dep.depid
)
)
WHERE rank = 2
""")

kpiDF.show()