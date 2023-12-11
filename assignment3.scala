val path = "hdfs:///tmp/Aryan/file1.csv"

val df = spark.read.option("header", "true").csv(path)

val df1 = df.write.partitionBy("category").bucketBy(10, "device_code")

val df2 = df1.sortBy("r4g_state").saveAsTable("newTable")

val df3 = spark.sql("DESCRIBE FORMATTED newTable")

df3.show()

val df4 = spark.sql("SHOW TBLPROPERTIES newTable")

df4.show()

val numPart = spark.sql("SHOW PARTITIONS newTable").count()

println(numPart)
