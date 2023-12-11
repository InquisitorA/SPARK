val path = "hdfs:///tmp/Aryan/file1.csv"
val hiveTable = "table1"

val df = spark.read.option("header", "true").csv(path)
df.show()

val df1 = df.withColumnRenamed("jc_code", "column1")
df1.show()

df1.write.mode("overwrite").saveAsTable(hiveTable)

val df2 = spark.table(hiveTable)
df2.show()

val count = spark.sql(s"SELECT COUNT(*) FROM $hiveTable").show()