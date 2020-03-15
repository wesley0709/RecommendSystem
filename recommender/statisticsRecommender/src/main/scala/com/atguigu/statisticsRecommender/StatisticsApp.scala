package com.atguigu.statisticsRecommender

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.FloatType

object StatisticsApp extends App{

  val RATINGS_COLLECTION_NAME = "Rating"
  val MOVIES_COLLECTION_NAME = "Movie"

  val params = scala.collection.mutable.Map[String, Any]()
  params += "spark.cores" -> "local[*]"
  params += "mongo.uri" -> "mongodb://linux:27017/recom"
  params += "mongo.db" -> "recom"

  val conf = new SparkConf().setAppName("statisticsRecommender").setMaster(params("spark.cores").asInstanceOf[String])
    .set("spark.executor.memory","4G")
  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  implicit val mongoConf = new MongoConfig(params("mongo.uri").asInstanceOf[String], params("mongo.db").asInstanceOf[String])

  val ratings = spark.read
    .option("uri", mongoConf.uri)
    .option("collection", RATINGS_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Rating]
    .cache

  val movies = spark.read
    .option("uri", mongoConf.uri)
    .option("collection", MOVIES_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie]
    .cache

  ratings.createOrReplaceTempView("ratings")

  //statisticsRecommender.rateMore(spark)
  //statisticsRecommender.rateMoreRecently(spark)
  statisticsRecommender.averageMovieScore(spark)(movies)

  ratings.unpersist()

  spark.close()

}
