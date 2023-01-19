#!/bin/bash

masterIP="192.168.0.1"
masterWorkerPort="65509"
slaveWorkerPort="65510"
webUIPort="8080"
masterSparkPort="7077"
masterHDFSPort="9000"

cores="8"
memory="8g"

jar="advanced-db-taxi-routes_2.12-1.0.jar"
class="AdvancedDBBenchmarker"
deployMode="client"

firstResult="/home/user/ADB-code/results/one-worker"
secondResult="/home/user/ADB-code/results/two-workers"

# Build Scala, put jar to hdfs
cd /home/user/ADB-code
~/sbt/bin/sbt package
hdfs dfs -put -f ~/ADB-code/target/scala-2.12/$jar /jars/$jar
# One worker
spark-daemon.sh start org.apache.spark.deploy.worker.Worker 1 \
--webui-port $webUIPort --port $masterWorkerPort --cores $cores --memory $memory spark://$masterIP:$masterSparkPort

# Submit
$SPARK_HOME/bin/spark-submit \
--class $class --master spark://$masterIP:$masterSparkPort --deploy-mode $deployMode \
hdfs://$masterIP:$masterHDFSPort//jars/$jar $firstResult

for i in 1 2 3 3RDD 4 5
do
    mv -f  $firstResult/Query${i}/part* $firstResult/Query${i}.csv
    rm -rf $firstResult/Query${i}
done
# Latex table

# Two workers
secondWorkerCommand="$SPARK_HOME/sbin/spark-daemon.sh start org.apache.spark.deploy.worker.Worker 2 \
--webui-port $webUIPort --port $slaveWorkerPort --cores $cores --memory $memory spark://$masterIP:$masterSparkPort"

ssh user@snf-34140 $secondWorkerCommand

# Submit
$SPARK_HOME/bin/spark-submit \
--class $class --master spark://$masterIP:$masterSparkPort --deploy-mode $deployMode \
hdfs://$masterIP:$masterHDFSPort//jars/$jar $secondResult

for i in 1 2 3 3RDD 4 5
do
    mv -f  $secondResult/Query${i}/part* $secondResult/Query${i}.csv
    rm -rf $secondResult/Query${i}
done

# Latex table