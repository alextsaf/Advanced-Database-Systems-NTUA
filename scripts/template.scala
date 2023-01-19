import org.apache.spark.sql.SparkSession
import java.io.FileWriter

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Main").getOrCreate()

    val sparkMasterIpPort = "192.168.0.1:9000"

    val fw = new FileWriter(s"${args(0)}");

    val taxiTripsDf = spark.read.parquet(s"hdfs://${sparkMasterIpPort}//data/yellow_tripdata_2022-*.parquet")
    val zoneLookupsDf = spark.read.csv(s"hdfs://${sparkMasterIpPort}//data/taxi+_zone_lookup.csv")
    
    val taxiTripsRdd = taxiTripsDf.rdd
    val zoneLookupsRdd = zoneLookupsDf.rdd

    fw.write(taxiTripsDf.count() + "\n")
    fw.write(zoneLookupsDf.count() + "\n")

    val times = executeAllQueries()

    times.foreach((x:Int) => fw.write(x + "\n"))

    fw.close()

    spark.stop()
  }

  def executeAllQueries(): Array[Int] = {
    var times = Array[Int]()

    times = for (query <- Array(q1 _, q2 _, q3Df _, q3Rdd _, q4 _, q5 _)) yield timeQuery(query)

    return times
  }

  def timeQuery(query: () => Unit): Int = {
    val start = System.nanoTime()

    query()

    val end = System.nanoTime()

    return (end - start).toInt
  }

  def q1(): Unit = {
    println("Query 1")
  }
  
  def q2(): Unit = {
    println("Query 2")
  }

  def q3Df(): Unit = {
    println("Query 3 - With dataframes")
  }

  def q3Rdd(): Unit = {
    println("Query 3 - With RDDs")
  }

  def q4(): Unit = {
    println("Query 4")
  }

  def q5(): Unit = {
    println("Query 5")
  }
}
