package utils

import java.lang.management.ManagementFactory

import org.apache.spark.sql.SparkSession

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
