package com.atguigu.offline

import org.apache.spark.SparkConf

/**
  * @author ${user.name}
  */
object RecommenderTrainerApp extends App{

  val params = scala.collection.mutable.Map[String, Any]()
  params += "spark.cores" -> "local[*]"
  params += "mongo.uri" -> "mongodb://linux:27017/recom"
  params += "mongo.db" -> "recom"
  params += "maxRecommendations" -> ALSTrainer.MAX_RECOMMENDATIONS.toString

  implicit val conf = new SparkConf().setAppName("RecommenderTrainerApp").setMaster(params("spark.cores").asInstanceOf[String]).set("spark.executor.memory","6G")

  implicit val mongoConf = new MongoConfig(params("mongo.uri").asInstanceOf[String], params("mongo.db").asInstanceOf[String])

  val maxRecommendations = params("maxRecommendations").asInstanceOf[String].toInt

  ALSTrainer.calculateRecs(maxRecommendations)

}
