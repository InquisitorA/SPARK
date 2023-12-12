val path = "hdfs:///tmp/Aryan/test.csv.gz"

val df= spark.read.option("compression", "gzip").csv(path)

val cleanedDF = df.toDF(df.columns.map(_.replaceAll("_", "")): _*)

cleanedDF.show()

cleanedDF.write.mode("overwrite").option("path", "/tmp/Aryan/tables").partitionBy("c1").save()

spark.sql("SELECT * FROM aryan_db.newTable")