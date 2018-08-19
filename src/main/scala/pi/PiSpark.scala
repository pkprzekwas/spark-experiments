package pi

import org.apache.log4j.{Level, Logger}

import scala.math.random
import utils.SparkUtils.getSession


object PiSpark {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = getSession("Pi estimation")

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

