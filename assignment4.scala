val path = "hdfs:///tmp/Aryan/test.csv"

val df1 = spark.read.option("header", "true").csv(path)
val df2 = df1.toDF(df1.columns.map(_.replaceAll("[^a-zA-Z0-9]", "")): _*)

val writePath = "hdfs:///tmp/spark_assignment_Aryan"

df2.write.parquet(writePath)

val df3 = spark.read.parquet(writePath)

df3.createOrReplaceTempView("newView")

spark.sql("SELECT COUNT(*) FROM newView").show()

//run the hql file/query from beeline

spark.sql("SELECT COUNT(*) FROM aryan_db.hiveTable").show()

df1.count()

df2.count()

df3.count()