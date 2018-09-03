package twitter

import helpers.Context
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object TweetStream extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val data_stream = sparkSession
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "twitter_stream")
    .option("startingOffsets", "latest")
    .load

  val schema = StructType(Seq(
    StructField("created_at", StringType, true),
    StructField("id", StringType, true),
    StructField("id_str", StringType, true),
    StructField("text", StringType, true),
    StructField("retweet_count", StringType, true),
    StructField("favorite_count", StringType, true),
    StructField("favorited", StringType, true),
    StructField("retweeted", StringType, true),
    StructField("lang", StringType, true),
    StructField("location", StringType, true)
  ))

  import org.apache.spark.sql.functions._

  val data_stream_transformed = data_stream
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"),schema=schema).as("tweet"))
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

  data_stream_transformed
    .writeStream
    .format("console")
    .option("truncate","true")
    .trigger(Trigger.Continuous("5 second"))
    .start
    .awaitTermination
}
