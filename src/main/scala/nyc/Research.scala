package nyc

import org.apache.log4j.{Level, Logger}
import utils.SparkUtils.getSession

object Research {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = getSession("nyc")

  }
}

