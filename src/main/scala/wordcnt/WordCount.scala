package wordcnt

import org.apache.spark.{SparkContext, SparkConf}

object WordCount extends App {
  val config = new SparkConf()
    .setAppName("WordCount")
  val sc = new SparkContext(config)

  val text = sc.textFile("hdfs://ip-172-31-45-44.eu-west-1.compute.internal:8020/tmp/pan-tadeusz.txt")
  val counts = text
    .flatMap(line => line.toLowerCase.split("\\W+"))
    .map(word => (word, 1))
    .reduceByKey(_ + _)

  counts.saveAsTextFile("hdfs://ip-172-31-45-44.eu-west-1.compute.internal:8020/tmp/out")
}
