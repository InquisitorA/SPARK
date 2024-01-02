# SPARK

Scala code can be coded directly from the spark-shell or loaded into the shell using "load:"

`spark-shell`

//To execute scala scripts use scalac compiler:

scalac test.scala

//Use the following command to pack it into a jar file

jar cf test.jar *.class

//Finally use spark-submit to run your jar file

spark-submit --class YourMainClass --master <master-url> --deploy-mode <deploy-mode> test.jar

# HIVE

//Use the beeline command to get into the child node from the master to access the hive terminal

//Code your hive queries


