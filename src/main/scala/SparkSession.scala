/* SparkSessionTest.scala */
import org.apache.spark.sql.SparkSession

object SparkSessionTest {
  def main(args: Array[String]) {
    val logFile = "/home/user/spark-3.1.3-bin-hadoop2.7/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }
}