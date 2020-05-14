#! /bin/sh

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <private key> <remote pc>"
    exit -1
fi


PRI_KEY=$1
REMOTE_PC=hadoop@$2

JAR_MR=out/artifacts/BigDataMR/BigDataMR.jar
JAR_SPARK=out/artifacts/BigDataSpark/BigDataSpark.jar
DIR_HQL=./hive


scp -i $PRI_KEY deploy.sh $REMOTE_PC:~
ssh -i $PRI_KEY $REMOTE_PC 'chmod u+x deploy.sh'
ssh -i $PRI_KEY $REMOTE_PC './deploy.sh' &



# send jars and hql files

scp -i $PRI_KEY $JAR_MR $REMOTE_PC:~/BigDataMR.jar &
scp -i $PRI_KEY $JAR_SPARK $REMOTE_PC:~/BigDataSpark.jar &
scp -i $PRI_KEY -r $DIR_HQL $REMOTE_PC:~
wait

# copy and start do-benchmarks.sh script

scp -i $PRI_KEY bench*.sh $REMOTE_PC:~
scp -i $PRI_KEY ./all-benchmarks.sh $REMOTE_PC:~
ssh -i $PRI_KEY $REMOTE_PC 'chmod u+x all-benchmarks.sh'
ssh -i $PRI_KEY $REMOTE_PC 'chmod u+x bench*.sh'

ssh -i $PRI_KEY $REMOTE_PC 'rm ./deploy.sh'
