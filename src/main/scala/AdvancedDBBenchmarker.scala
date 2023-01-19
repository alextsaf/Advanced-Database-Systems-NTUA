/* AdvancedDBBenchmarker.scala */
import org.apache.spark.sql.SparkSession
import java.io._

object AdvancedDBBenchmarker {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder
                            .appName("Advanced DB Taxi Routes")
                            .getOrCreate()

    val sparkMasterIpPort = "192.168.0.1:9000"

    val yellowDF = spark.read.parquet(s"hdfs://${sparkMasterIpPort}//data/tripdata/yellow_tripdata_2022-*.parquet")
    val zoneLookupsDf = spark.read.option("header","true").csv(s"hdfs://${sparkMasterIpPort}//data/taxi+_zone_lookup.csv")


    zoneLookupsDf.createOrReplaceTempView("Zone_Lookups")
    yellowDF.na.drop().createOrReplaceTempView("Taxi_Trips_Temp")
    
    // Add the zone names as columns and drop data not in the first six months of 2022
    val taxiTripsDf = spark.sql("""SELECT PU.Zone as PUZone, Drop.Zone as DOZone, Taxi_Trips_Temp.* 
                            FROM Taxi_Trips_Temp INNER JOIN Zone_Lookups as Drop ON Drop.LocationID == DOLocationID
                            INNER JOIN Zone_Lookups PU ON PU.LocationID == PULocationID
                            WHERE MONTH(tpep_pickup_datetime) between 1 and 6
                            AND MONTH(tpep_dropoff_datetime) between 1 and 6
                            AND PULocationID != 265
                            AND PULocationID != 264
                            AND DOLocationID != 265
                            AND DOLocationID != 264""")

    val zoneLookupsRdd = zoneLookupsDf.rdd
    val taxiTripsRdd = taxiTripsDf.rdd

    taxiTripsDf.createOrReplaceTempView("Taxi_Trips")
    
    // query1()
    // query2()
    query3SQL()
    query3RDD()

    spark.stop()

      def query1() : Unit = {
        val maxTipAmountDF = spark.sql(s"""SELECT MAX(Tip_amount) as max_tip_amount 
                                        FROM Taxi_Trips 
                                        WHERE DOZone  == 'Battery Park' AND MONTH(tpep_pickup_datetime) == 3""")
        maxTipAmountDF.createOrReplaceTempView("Max_Tip")

        val query1DF = spark.sql(s"""SELECT VendorID, PUZone, DOZone, tpep_pickup_datetime, Trip_distance, Total_amount, max_tip_amount 
                                  FROM Taxi_Trips INNER JOIN Max_Tip ON Tip_amount == max_tip_amount
                                  WHERE DOZone  == 'Battery Park' AND MONTH(tpep_pickup_datetime) == 3""")
        query1DF.show()
        query1DF.repartition(1).write.option("header","true").mode("overwrite").csv(s"${args(0)}/Query1")
      }

      def query2() : Unit = {
        val maxTollAmountperMonth = spark.sql(s"""SELECT MONTH(tpep_pickup_datetime) as month, MAX(Tolls_amount) as max_tolls_amount 
                                        FROM Taxi_Trips 
                                        GROUP BY MONTH(tpep_pickup_datetime)
                                        HAVING max_tolls_amount > 0""")
        maxTollAmountperMonth.createOrReplaceTempView("Max_Tolls")

        val query2DF = spark.sql(s"""SELECT month, VendorID, PUZone, DOZone, tpep_pickup_datetime, Trip_distance, Total_amount, max_tolls_amount 
                                  FROM Taxi_Trips INNER JOIN Max_Tolls ON Tolls_amount == max_tolls_amount
                                  ORDER BY month""")
        query2DF.show()
        query2DF.repartition(1).write.option("header","true").mode("overwrite").csv(s"${args(0)}/Query2")
      }

      def query3SQL() : Unit = {
        val query3DF = spark.sql(s"""SELECT MONTH(tpep_pickup_datetime) as trip_month, (CASE WHEN DAY(tpep_pickup_datetime) > 15 THEN 2 ELSE 1 END) as half_of_the_month, AVG(Trip_distance) as average_trip_distance
                                  FROM Taxi_Trips
                                  WHERE PULocationID != DOLocationID
                                  GROUP BY trip_month, half_of_the_month
                                  ORDER BY trip_month, half_of_the_month""")
        query3DF.show()
        query3DF.repartition(1).write.option("header","true").mode("overwrite").csv(s"${args(0)}/Query3")
      }

      def query3RDD() : Unit = {
        val rddColumns = taxiTripsDf.columns
        val puDatetimeIndex = rddColumns.indexOf("tpep_pickup_datetime")
        val puLocationIDIndex = rddColumns.indexOf("PULocationID")
        val doLocationIDIndex = rddColumns.indexOf("DOLocationID")
        val tripDistanceIDIndex = rddColumns.indexOf("trip_distance")


        def rowParser(row : org.apache.spark.sql.Row) = {
          val rowDate = row(puDatetimeIndex).toString.split(' ')(0)
          val rowDateDay = rowDate.split('-')
          val month = rowDateDay(1).toInt
          val day = rowDateDay(2).toInt
          val tripDistance = row(tripDistanceIDIndex).toString.toFloat
          val halfOfTheMonth = if (day > 15) { 2 } else { 1 }
          
          ((month, halfOfTheMonth), (tripDistance, 1))
        }
        val query3RDD = taxiTripsRdd.filter(row => row(puLocationIDIndex) != row(doLocationIDIndex))
                                          .map(row => rowParser(row))
                                          .reduceByKey((key1 , key2) => 
                                          ((key1._1*key1._2 + key2._1*key2._2)/(key1._2+key2._2), key1._2+key2._2))

        query3RDD.foreach(row => print(s"${row._1._1}, ${row._1._2}, ${row._2._1}\n"))

        query3RDD.map(row => (row._1._1, row._1._2, row._2._1)).repartition(1).saveAsTextFile(s"${args(0)}/Query3RDD")
      }
  }

}