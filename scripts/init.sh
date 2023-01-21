#! /bin/bash

mkdir -p ~/data/tripdata
hdfs dfs -mkdir -p /data/tripdata
GREEN='\033[0;32m'
NC='\033[0m' # No Color
for month in {1..6}
do
    echo "Downloading month ${month} data..."
    wget -nc "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-0${month}.parquet"
    mv -f "yellow_tripdata_2022-0${month}.parquet" ~/data/tripdata

    echo "Loading month's data to HDFS..."
    hdfs dfs -put -f ~/data/tripdata/yellow_tripdata_2022-0${month}.parquet "/data/tripdata/"

    echo -e "${GREEN}DONE [${month}/7]${NC}"
done
echo "Downloading zone lookups"
wget -nc "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
mv -f "taxi+_zone_lookup.csv" ~/data

echo "Loading zone lookups in HDFS"
hdfs dfs -put -f ~/data/taxi+_zone_lookup.csv "/data/"

echo -e "${GREEN}DONE [7/7]${NC}"

pip3.8 install pandas
pip3.8 install tabulate

