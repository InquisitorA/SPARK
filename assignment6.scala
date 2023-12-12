val df1 = spark.read.option("header", "true").csv("hdfs:///tmp/Aryan/Ass6/set1.csv")

df1.show()

val df2 = spark.read.option("header", "true").csv("hdfs:///tmp/Aryan/Ass6/set2.csv")

df2.show()

df1.write.mode("overwrite").parquet("hdfs:///tmp/Aryan/Ass6/df1.parquet")

df2.write.mode("overwrite").parquet("hdfs:///tmp/Aryan/Ass6/df2.parquet")

val df1_ = spark.read.parquet("hdfs:///tmp/Aryan/Ass6/df1.parquet")

val df2_ = spark.read.parquet("hdfs:///tmp/Aryan/Ass6/df2.parquet")

val df1_r = df1_.withColumn("rank", monotonically_increasing_id())

val df2_r = df2_.withColumn("rank", monotonically_increasing_id())

val cachedDF = df1_r.cache()

import org.apache.spark.storage.StorageLevel

val persistedDF = df2_r.persist(StorageLevel.DISK_ONLY)

val df_left = df1_r.join(df2_r, Seq("rank"), "left").show()

cachedDF.createOrReplaceTempView("tempview1")
persistedDF.createOrReplaceTempView("tempview2")

val df_left_sql = spark.sql("SELECT * FROM tempview1 LEFT JOIN tempview2 ON tempview1.rank = tempview2.rank").limit(10).show()

//similarly do the same for rest