package twitter

import helpers.Context
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object TweetStream extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val dataStream = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "twitter_stream")
    .option("startingOffsets", "latest")
    .load

  val schema = StructType(Seq(
    StructField("created_at", StringType, nullable = true),
    StructField("id", StringType, nullable = true),
    StructField("id_str", StringType, nullable = true),
    StructField("text", StringType, nullable = true),
    StructField("retweet_count", StringType, nullable = true),
    StructField("favorite_count", StringType, nullable = true),
    StructField("favorited", StringType, nullable = true),
    StructField("retweeted", StringType, nullable = true),
    StructField("lang", StringType, nullable = true),
    StructField("location", StringType, nullable = true)
  ))

  import org.apache.spark.sql.functions._

  var dataStreamTransformed = dataStream
    .withWatermark("timestamp","5 minutes")

  dataStreamTransformed = dataStream
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema=schema).as("tweet"))
    .selectExpr(
      "tweet.created_at",
      "cast (tweet.id as long)",
      "tweet.id_str",
      "tweet.text",
      "cast (tweet.retweet_count as integer)",
      "cast (tweet.favorite_count as integer)" ,
      "cast(tweet.favorited as boolean)" ,
      "cast(tweet.retweeted as boolean)",
      "tweet.lang as language_code"
    )

  dataStreamTransformed = dataStreamTransformed
    .groupBy("language_code")
    .agg(sum("favorite_count"), count("id"))

  dataStreamTransformed
    .writeStream
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .outputMode("update")
    .format("console")
    .option("truncate","true")
    .start
    .awaitTermination
}
