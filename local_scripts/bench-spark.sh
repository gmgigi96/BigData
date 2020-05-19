echo "#########################################"
echo "############ BENCHMARK SPARK ############"
echo "#########################################"

TIME=/bin/time
JAR_SPARK=/home/gianmaria/Documenti/Proj/BigData/out/artifacts/BigDataSpark/BigDataSpark.jar
HISTORICAL_STOCKS_DATASET=/input/hs/historical_stocks.csv
SETTINGS=""
OUT_DIR=/output_spark


function check {
    if [ $1 != 0 ]; then
        echo $2
        echo $2 >> ~/status.txt
    fi
}


rm -rf SparkBenchmarks
hdfs dfs -rm -r /temp_spark
hdfs dfs -rm -r /output_spark

hdfs dfs -mkdir /temp_spark
hdfs dfs -mkdir /output_spark
mkdir SparkBenchmarks


for file in `hdfs dfs -ls -C /input/hsp*`; do

    LINES=`echo $file | sed -e 's/\/input\/hsp\(.*\)\/hsp_.*\.csv/\1/'`

    # job 1
    echo -e "\n\t############ JOB1 SPARK - $file ############\n"

    $TIME -f "$LINES\t%e" -a -o SparkBenchmarks/job1.txt spark-submit --class "proj.spark.StockStats" $SETTINGS $JAR_SPARK $file $OUT_DIR/output_spark/job1_spark_$LINES
    check $? "Spark job1 failed with $file file"

    LINES=`echo $file | sed -e 's/\/input\/hsp\(.*\)\/hsp_.*\.csv/\1/'`

    # job 2
    echo -e "\n\t############ JOB2 SPARK - $file ############\n"

    $TIME -f "$LINES\t%e" -a -o SparkBenchmarks/job2.txt spark-submit --class "proj.spark.SectorTrend" $SETTINGS $JAR_SPARK $file $HISTORICAL_STOCKS_DATASET $OUT_DIR/output_spark/job2_spark_$LINES
    check $? "Spark job2 failed with $file file"

    # job 3
    echo -e "\n\t############ JOB3 SPARK - $file ############\n"

    LINES=`echo $file | sed -e 's/\/input\/hsp\(.*\)\/hsp_.*\.csv/\1/'`

    $TIME -f "$LINES\t%e" -a -o SparkBenchmarks/job3.txt spark-submit --class "proj.spark.SameTrend" $SETTINGS $JAR_SPARK $file $HISTORICAL_STOCKS_DATASET $OUT_DIR/output_spark/job3_spark_$LINES
    check $? "Spark job3 failed with $file file"

done
