package pi

import java.lang.management.ManagementFactory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.math.random

object SparkUtils {
  val isIDE: Boolean = {
    ManagementFactory
      .getRuntimeMXBean
      .getInputArguments
      .toString
      .contains("IDEA")
  }

  def getSession(name: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(name)
    if (isIDE) {
      spark.master("local[4]")
    }
    spark.getOrCreate()
  }
}

object PiSpark {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkUtils.getSession("Pi estimation")

    val slices = if (args.length > 0) args(0).toInt else 2
    val n = math.min(100000L * slices, Int.MaxValue).toInt

    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)

    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }
}

