echo "########################################"
echo "############ BENCHMARK HIVE ############"
echo "########################################"

TIME=/bin/time


function check {
    if [ $1 != 0 ]; then
        echo $2
        echo $2 >> ~/status.txt
    fi
}

rm -rf HiveBenchmarks
mkdir HiveBenchmarks

HISTORICAL_STOCKS_DATASET=/input/hs


for file in `hdfs dfs -ls -C -d /input/hsp*`; do

    LINES=`echo $file | sed -e 's/\/input\/hsp\(.*\)/\1/'`

    # job 1
    $TIME -f "$LINES\t%e" -a -o HiveBenchmarks/job1.txt hive --hiveconf input=$file -f hive/job1/job1.hql
    check $? "Hive job1 failed with $file file"

    # job 2
    $TIME -f "$LINES\t%e" -a -o HiveBenchmarks/job2.txt hive --hiveconf input1=$file --hiveconf input2=$HISTORICAL_STOCKS_DATASET -f hive/job2/job2.hql
    check $? "Hive job2 failed with $file file"

    # job 3
    $TIME -f "$LINES\t%e" -a -o HiveBenchmarks/job3.txt hive --hiveconf input1=$file --hiveconf input2=$HISTORICAL_STOCKS_DATASET -f hive/job3/job3.hql
    check $? "Hive job3 failed with $file file"

done