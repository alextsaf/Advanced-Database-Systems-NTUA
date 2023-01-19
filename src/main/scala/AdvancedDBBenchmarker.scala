/* AdvancedDBBenchmarker.scala */
import org.apache.spark.sql.SparkSession
import java.io._

object AdvancedDBBenchmarker {
  def main(args: Array[String]) {
    
    val spark = SparkSession.builder
                            .appName("Advanced DB Taxi Routes")
                            .getOrCreate()

    val sparkMasterIpPort = "192.168.0.1:9000"

    val taxiTripsDf = spark.read.parquet(s"hdfs://${sparkMasterIpPort}//data/tripdata/yellow_tripdata_2022-*.parquet")
    val zoneLookupsDf = spark.read.option("header","true").csv(s"hdfs://${sparkMasterIpPort}//data/taxi+_zone_lookup.csv")
    
    val taxiTripsRdd = taxiTripsDf.rdd
    val zoneLookupsRdd = zoneLookupsDf.rdd

    zoneLookupsDf.createOrReplaceTempView("Zone_Lookups")
    taxiTripsDf.createOrReplaceTempView("Taxi_Trips")

    query1()

    spark.stop()

      def query1() : Unit = {
        // val pw = new PrintWriter(new File(s"${args(0)}"))
        val batteryParkDF = spark.sql("SELECT LocationID FROM Zone_Lookups WHERE Zone  == 'Battery Park' ")
        val batteryParkID = batteryParkDF.first().getString(0)

        val maxTipAmountDF = spark.sql(s"""SELECT MAX(Tip_amount) as max_tip_amount 
                                        FROM Taxi_Trips 
                                        WHERE DOLocationID  == $batteryParkID AND MONTH(tpep_pickup_datetime) == 3""")
        maxTipAmountDF.createOrReplaceTempView("Max_Tip")

        val query1DF = spark.sql(s"""SELECT VendorID, PULocationID, tpep_pickup_datetime, Trip_distance, Total_amount, max_tip_amount 
                                  FROM Taxi_Trips INNER JOIN Max_Tip ON Tip_amount == max_tip_amount
                                  WHERE DOLocationID  == $batteryParkID AND MONTH(tpep_pickup_datetime) == 3""")
        // pw.close()
        query1DF.write.mode(SaveMode.Overwrite).csv(s"${args(0)}")

      }
  }

}