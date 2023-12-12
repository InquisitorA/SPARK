SHOW DATABASES;

CREATE DATABASE aryan_db;

USE aryan_db;

SHOW TABLES;

CREATE EXTERNAL TABLE IF NOT EXISTS aryan_db.hiveTable(description STRING, industry STRING, level STRING, linecode STRING)
STORED AS PARQUET LOCATION "hdfs:///tmp/spark_assignment_Aryan;

SELECT * FROM aryan_db.hiveTable limit 10;