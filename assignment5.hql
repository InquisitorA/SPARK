CREATE TABLE EXTERNAL TABLE aryan_db.newTable(c0 STRING, c1 STRING, c2 STRING, c3 STRING, c4 STRING, c5 STRING)
PARTITIONED BY(c1 STRING) LOCATION "tmp/Aryan/tables";