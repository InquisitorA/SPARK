The assignments are done on a canary environment.
ssh is used to access the main node which has various dependencies like spark, scala.
Beeline is used to access another node in which hive system is present.

Entire server runs on hdfs and files can be accessed from anywhere with proper permissions.

hdfs dfs -ls /tmp          //shows all directories in tmp folder

hdfs dfs -copyFromLocal src dest    //to copy from canary local to hdfs system

hdfs dfs -copyToLocal src dest      //to copy from hdfs to local

A user from another node will be treated as anonymous user and must have other permissions to r/w/x a file hence use
hdfs dfs -chmod -R 777 directory   //gives r, w, x access to everyone(user, group, other)

The above commands can also be executed from the hive beeline but must be preceeded with a "!sh"
eg: !sh hdfs dfs -chmod -R 777 /tmp/spark_assignment_Aryan
