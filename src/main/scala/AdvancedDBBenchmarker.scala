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
    

      def query1() : Float = {

        val start = System.nanoTime()

        val maxTipAmount = """(SELECT MAX(Tip_amount) as max_tip_amount 
                              FROM Taxi_Trips 
                              WHERE DOZone  == 'Battery Park' AND MONTH(tpep_pickup_datetime) == 3)"""

        val query1DF = spark.sql(s"""SELECT VendorID, PUZone, DOZone, tpep_pickup_datetime, Trip_distance, Total_amount, max_tip_amount 
                                  FROM Taxi_Trips INNER JOIN ${maxTipAmount} ON Tip_amount == max_tip_amount
                                  WHERE DOZone  == 'Battery Park' AND MONTH(tpep_pickup_datetime) == 3""")
        query1DF.show()

        val time = System.nanoTime() - start

        query1DF.repartition(1).write.option("header","true").mode("overwrite").csv(s"${args(0)}/Query1")

        time
      }

      def query2() : Float = {

        val start = System.nanoTime()

        val maxTollAmountperMonth = s"""(SELECT MONTH(tpep_pickup_datetime) as month, MAX(Tolls_amount) as max_tolls_amount 
                                        FROM Taxi_Trips 
                                        GROUP BY MONTH(tpep_pickup_datetime)
                                        HAVING max_tolls_amount > 0)"""


        val query2DF = spark.sql(s"""SELECT month, VendorID, PUZone, DOZone, tpep_pickup_datetime, Trip_distance, Total_amount, max_tolls_amount 
                                  FROM Taxi_Trips INNER JOIN ${maxTollAmountperMonth} ON Tolls_amount == max_tolls_amount
                                  ORDER BY month""")
        
        query2DF.show()
        
        val time = System.nanoTime() - start
        
        query2DF.repartition(1).write.option("header","true").mode("overwrite").csv(s"${args(0)}/Query2")
      
        time   
      
      }

      def query3SQL() : Float = {

        val start = System.nanoTime()

        val query3DF = spark.sql(s"""SELECT MONTH(tpep_pickup_datetime) as trip_month, (CASE WHEN DAY(tpep_pickup_datetime) > 15 THEN 2 ELSE 1 END) as half_of_the_month, AVG(Trip_distance) as average_trip_distance, AVG(Total_amount) as average_total_amount
                                  FROM Taxi_Trips
                                  WHERE PULocationID != DOLocationID
                                  GROUP BY trip_month, half_of_the_month
                                  ORDER BY trip_month, half_of_the_month""")
       
        query3DF.show()
       
        val time = System.nanoTime() - start

        query3DF.repartition(1).write.option("header","true").mode("overwrite").csv(s"${args(0)}/Query3")
      
        time   
      
      }

      def query3RDD() : Float = {

        val start = System.nanoTime()

        val rddColumns = taxiTripsDf.columns
        val puDatetimeIndex = rddColumns.indexOf("tpep_pickup_datetime")
        val puLocationIDIndex = rddColumns.indexOf("PULocationID")
        val doLocationIDIndex = rddColumns.indexOf("DOLocationID")
        val tripDistanceIndex = rddColumns.indexOf("trip_distance")
        val totalAmountIndex = rddColumns.indexOf("total_amount")


        def rowParser(row : org.apache.spark.sql.Row) = {
          val rowDate = row(puDatetimeIndex).toString.split(' ')(0)
          val rowDateDay = rowDate.split('-')
          val month = rowDateDay(1).toInt
          val day = rowDateDay(2).toInt
          val tripDistance = row(tripDistanceIndex).toString.toFloat
          val totalAmount = row(totalAmountIndex).toString.toFloat
          val halfOfTheMonth = if (day > 15) { 2 } else { 1 }
          
          ((month, halfOfTheMonth), (tripDistance, 1, totalAmount))
        }

        val query3RDD = taxiTripsRdd.filter(row => row(puLocationIDIndex) != row(doLocationIDIndex))
                                          .map(row => rowParser(row))
                                          .reduceByKey((key1 , key2) => 
                                          ((key1._1*key1._2 + key2._1*key2._2)/(key1._2+key2._2), key1._2+key2._2, (key1._3*key1._2 + key2._3*key2._2)/(key1._2+key2._2)))

        query3RDD.foreach(row => print(s"${row._1._1}, ${row._1._2}, ${row._2._1}, , ${row._2._3}\n"))

        val time = System.nanoTime() - start

        query3RDD.map(row => s"${row._1._1},${row._1._2},${row._2._1},${row._2._3}").repartition(1).saveAsTextFile(s"${args(0)}/Query3RDD")
      
        time   
      
      }

      def query4(): Float = {

        val start = System.nanoTime()

        val maxPassengersPerHour = """(SELECT MAX(passenger_count) AS max_passengers, HOUR(tpep_pickup_datetime) as hour, WEEKDAY(tpep_pickup_datetime) as weekday 
                                      FROM Taxi_Trips 
                                      GROUP BY hour, weekday
                                      ORDER BY max_passengers DESC)"""

        val orderedHours = s"""(SELECT max_passengers, hour, weekday, row_number() OVER (PARTITION BY weekday ORDER BY max_passengers DESC) AS weekday_rank
                                FROM ${maxPassengersPerHour})"""

        // Limit results to 3
        val query4DF = spark.sql(s"""SELECT weekday, hour, weekday_rank as rank, max_passengers
                              FROM $orderedHours
                              WHERE weekday_rank <=3
                              ORDER BY weekday ASC, rank ASC""")
       
        query4DF.show()
       
        val time = System.nanoTime() - start
       
        query4DF.repartition(1).write.option("header","true").mode("overwrite").csv(s"${args(0)}/Query4")
        
        time   
      
      }

      def query5(): Float = {

        val start = System.nanoTime()

        val tipPercentages = """(SELECT MONTH(tpep_pickup_datetime) AS Month, DAY(tpep_pickup_datetime) AS Day, (SUM(Tip_amount)/SUM(Fare_amount))*100 AS Percentage
                                FROM Taxi_Trips 
                                WHERE MONTH(tpep_pickup_datetime) <= 6 
                                GROUP BY Day, Month
                                ORDER BY Month ASC, Percentage DESC)"""

        val rankQueryString = s"""(SELECT Percentage, Day, Month, row_number() OVER (PARTITION BY Month ORDER BY Percentage DESC) AS month_rank 
                                FROM ${tipPercentages})"""

        val query5DF = spark.sql(s"""SELECT Day, Month, Percentage, month_rank as Rank 
                              FROM ($rankQueryString) 
                              WHERE month_rank <=5 
                              ORDER BY Month ASC, month_rank ASC""")
        
        query5DF.show()
        
        val time = System.nanoTime() - start
        
        query5DF.repartition(1).write.option("header","true").mode("overwrite").csv(s"${args(0)}/Query5")      

        time   
      }


    val columns = Seq("Query", "Time")
    
    val times = Seq(("Query1", query1()), ("Query2", query2()), ("Query3 SQL", query3SQL()), ("Query3 RDD", query3RDD()), ("Query4", query4()), ("Query5", query5()))

    val timesDF = spark.createDataFrame(times).toDF(columns:_*)
    
    timesDF.repartition(1).write.mode("append").csv(s"${args(0)}/Times")

    spark.stop()

  }

}