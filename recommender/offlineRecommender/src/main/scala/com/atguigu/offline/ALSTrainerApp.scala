package com.atguigu.offline

import com.atguigu.offline.ALSTrainer.{MOVIES_COLLECTION_NAME, RATING_COLLECTION_NAME}
import com.atguigu.offline.RecommenderTrainerApp.params
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.sqrt

object ALSTrainerApp extends App{

  def parameterAdjust(trainData: RDD[Rating], realRatings: RDD[Rating]): (Int, Double, Double) = {
    val evaluations =
      for (rank   <- Array(10,  50);
           lambda <- Array(1.0, 0.0001);
           alpha  <- Array(1.0, 40.0))
        yield {
          val model = ALS.trainImplicit(trainData, rank, 10, lambda, alpha)
          val rmse = computeRmse(model, realRatings)
          ((rank, lambda, alpha), rmse)
        }
    val ((rank, lambda, alpha), rmse) = evaluations.sortBy(_._2).head
    println("After parameter adjust, the best rmse = " + rmse)
    (rank, lambda, alpha)
  }

  def computeRmse(model: MatrixFactorizationModel, realRatings: RDD[Rating]): Double = {
    val testingData = realRatings.map{ case Rating(user, product, rate) =>
      (user, product)
    }

    val prediction = model.predict(testingData).map{ case Rating(user, product, rate) =>
      ((user, product), rate)
    }

    val realPredict = realRatings.map{case Rating(user, product, rate) =>
      ((user, product), rate)
    }.join(prediction)

    sqrt(realPredict.map{ case ((user, product), (rate1, rate2)) =>
      val err = rate1 - rate2
      err * err
    }.mean())//mean = sum(list) / len(list)
  }


  val params = scala.collection.mutable.Map[String, Any]()
  params += "spark.cores" -> "local[*]"
  params += "mongo.uri" -> "mongodb://linux:27017/recommender"
  params += "mongo.db" -> "recommender"

  val mongoConf = new MongoConfig(params("mongo.uri").asInstanceOf[String], params("mongo.db").asInstanceOf[String])

  val conf = new SparkConf().setAppName("RecommenderTrainerApp").setMaster(params("spark.cores").asInstanceOf[String]).set("spark.executor.memory","6G")

  val spark = SparkSession.builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  val ratings = spark.read
    .option("uri", mongoConf.uri)
    .option("collection", RATING_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .select($"mid",$"uid",$"score")
    .cache

  val users = ratings
    .select($"uid")
    .distinct
    .map(r => r.getAs[Int]("uid"))
    .cache

  val movies = spark.read
    .option("uri", mongoConf.uri)
    .option("collection", MOVIES_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .select($"mid")
    .distinct
    .map(r => r.getAs[Int]("mid"))
    .cache

  val trainData = ratings.map{ line =>
    Rating(line.getAs[Int]("uid"), line.getAs[Int]("mid"), line.getAs[Double]("score"))
  }.rdd.cache()

  println(parameterAdjust(trainData,trainData))

  spark.stop()
}
