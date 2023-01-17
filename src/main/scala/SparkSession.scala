/* AdvancedDBBenchmarker.scala */
import org.apache.spark.sql.SparkSession

object AdvancedDBBenchmarker {
  def main(args: Array[String]) {
    val logFile = "hdfs://192.168.0.1:9000/oute_enas.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile)
    val context = logData.first()
    println(context)
    spark.stop()
  }
}