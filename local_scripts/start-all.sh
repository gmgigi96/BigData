#! /bin/sh

INPUT_DIR=input

sudo systemctl start sshd
hdfs namenode -format
$HADOOP_HOME/sbin/start-all.sh

echo "Y" | hdfs namenode -format

hdfs dfs -mkdir -p /user/gianmaria
hdfs dfs -mkdir input
hdfs dfs -mkdir output
hdfs dfs -mkdir temp

# cartelle per hive
hdfs dfs -mkdir /tmp
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /tmp
hdfs dfs -chmod g+w /user/hive/warehouse
