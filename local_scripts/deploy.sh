#! /bin/sh

# genera file di diverse dimensioni
cd input

chmod 664 *.csv 
hdfs dfs -mkdir /input

lengths=(
       78125
      156250
      312500
      625000
     1250000
     2500000
     5000000
)

for i in "${lengths[@]}"; do
    echo "Generating hsp_${i}.csv"
    head -$i historical_stock_prices.csv > hsp_${i}.csv
    hdfs dfs -mkdir /input/hsp${i}
    echo "Coping hsp_${i}.csv into hdfs"
    hdfs dfs -put hsp_${i}.csv /input/hsp${i}
    rm hsp_${i}.csv
done

echo "Coping historical_stocks.csv into hdfs"
hdfs dfs -mkdir /input/hs
hdfs dfs -put historical_stocks.csv /input/hs

