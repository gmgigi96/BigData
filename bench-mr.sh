echo -e "\n##############################################"
echo "############ BENCHMARK MAP-REDUCE ############"
echo -e "##############################################\n"

TIME=/bin/time
JAR_MR=~/BigDataMR.jar


function check {
    if [ $1 != 0 ]; then
        echo $2
        echo $2 >> ~/status.txt
    fi
}


rm -rf MRBenchmarks
hdfs dfs -rm -r /temp_mr
hdfs dfs -rm -r /output_mr

hdfs dfs -mkdir /temp_mr
hdfs dfs -mkdir /output_mr
mkdir MRBenchmarks


MAIN_CLASS_JOB1=DailyHistoricalStockPrices
MAIN_CLASS_JOB2=SectorTrend
MAIN_CLASS_JOB3=SameTrendCompany

HISTORICAL_STOCKS_DATASET=/input/hs/historical_stocks.csv

for file in `hdfs dfs -ls -C /input/hsp*`; do

    LINES=`echo $file | sed -e 's/\/input\/hsp\(.*\)\/hsp_.*\.csv/\1/'`
    
    # job 1
    echo -e "\n\t############ JOB1 MR - $file ############\n"

    $TIME -f "$LINES\t%e" -a -o MRBenchmarks/job1.txt hadoop jar $JAR_MR proj/mapreduce/job1/$MAIN_CLASS_JOB1 $file /temp_mr/job1_$LINES /output_mr/job1_mr_$LINES
    check $? "MapReduce job1 failed with $file file"

    # job 2
    echo -e "\n\t############ JOB2 MR - $file ############\n"

    $TIME -f "$LINES\t%e" -a -o MRBenchmarks/job2.txt hadoop jar $JAR_MR proj/mapreduce/job2/$MAIN_CLASS_JOB2 $file $HISTORICAL_STOCKS_DATASET /temp_mr/job2_1_$LINES /temp_mr/job2_2_$LINES /output_mr/job2_mr_$LINES
    check $? "MapReduce job2 failed with $file file"

    # job 3
    echo -e "\n\t############ JOB3 MR - $file ############\n"

    $TIME -f "$LINES\t%e" -a -o MRBenchmarks/job3.txt hadoop jar $JAR_MR proj/mapreduce/job3/$MAIN_CLASS_JOB3 $file $HISTORICAL_STOCKS_DATASET /temp_mr/job3_1_$LINES /temp_mr/job3_2_$LINES /output_mr/job3_mr_$LINES
    check $? "MapReduce job3 failed with $file file"

done 
