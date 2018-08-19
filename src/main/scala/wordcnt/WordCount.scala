package wordcnt

import org.apache.spark.{SparkContext, SparkConf}

object WordCount extends App {
  val config = new SparkConf()
    .setAppName("WordCount")
    .setMaster("local[8]")
  val sc = new SparkContext(config)

  val text = sc.textFile("/tmp/in.txt")
  val counts = text
    .flatMap(line => line.toLowerCase.split("\\W+"))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.saveAsTextFile("/tmp/out")
}
