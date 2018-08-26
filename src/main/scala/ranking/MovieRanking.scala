package ranking

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

trait Context {
  lazy val sparkConf = new SparkConf()
    .setAppName("The Move dataset")
    .setMaster("local[*]")
    .set("spark.cores.max", "2")
  lazy val sparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()
}

object MovieRanking extends App with Context {
  Logger.getLogger("org").setLevel(Level.ERROR)
  import org.apache.spark.sql.functions._

  val dataPath = "/tmp/the-movies-dataset"
  val ratings = sparkSession
    .read
    .option("header","true")
    .option("inferSchema", "true")
    .csv(s"$dataPath/ratings.csv")
  ratings.createOrReplaceTempView("ratings")

  val globalAverageMovieRate = ratings
    .select(avg("rating"))
    .take(1)(0)(0)
  val minVoteNumToConsider = 25000

  val ranking = sparkSession.sql(
    s"""SELECT
      |  movieId,
      |  v as numberOfVotes,
      |  R as averageRate,
      |  (v/(v+m)) * R + (m/(v+m)) * C as rankingRate
      |FROM (
      |  SELECT
      |    movieId,
      |    $minVoteNumToConsider as m,
      |    $globalAverageMovieRate as C,
      |    count(*) as v,
      |    AVG(rating) as R
      |  FROM ratings
      |  GROUP BY movieId
      |)
    """.stripMargin)

  val movies = sparkSession
    .read
    .option("header","true")
    .option("inferSchema", "true")
    .csv(s"$dataPath/movies_metadata.csv")
    .select("id", "original_title")

  val moviesRatingsJoin = movies
    .join(
      ranking,
      movies.col("id") === ranking.col("movieId")
    )
    .filter("numberOfVotes > 1000")
    .orderBy(desc("averageRate"))
    .limit(100)

  val tsvWithHeaderOptions: Map[String, String] = Map(
    ("delimiter", "\t"),
    ("header", "true")
  )

  moviesRatingsJoin
    .coalesce(1)
    .write
    .mode(SaveMode.Overwrite)
    .options(tsvWithHeaderOptions)
    .csv("/tmp/mr-out")
}


