package com.atguigu.offline

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, WriteConcern => MongodbWriteConcern}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.jblas.DoubleMatrix


object ALSTrainer {

  val RATING_COLLECTION_NAME = "Rating"

  val MOVIES_COLLECTION_NAME = "Movie"

  val USER_RECS_COLLECTION_NAME = "UserRecs"

  val MOVIE_RECS_COLLECTION_NAME = "MovieRecs"

  val MAX_RATING = 5.0F
  val MAX_RECOMMENDATIONS = 100

  /**
    * 计算推荐数据
    * @param maxRecs
    * @param _conf
    * @param mongoConf
    */
  def calculateRecs(maxRecs: Int)(implicit _conf: SparkConf, mongoConf: MongoConfig): Unit = {
    val spark = SparkSession.builder()
      .config(_conf)
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

    val (rank, iterations, lambda) = (50, 5, 0.01)
    val model = ALS.train(trainData, rank, iterations, lambda)

    implicit val mongoClient = MongoClient(MongoClientURI(mongoConf.uri))

    calculateUserRecs(maxRecs, model, users, movies)
    calculateProductRecs(maxRecs, model, movies)

    mongoClient.close()

    spark.close()
  }


  /**
    * 计算为用户推荐的电影集合矩阵 RDD[UserRecommendation(id: Int, recs: Seq[Rating])]
    * @param maxRecs
    * @param model
    * @param users
    * @param products
    * @param mongoConf
    * @param mongoClient
    */
  private def calculateUserRecs(maxRecs: Int, model: MatrixFactorizationModel, users: Dataset[Int], products: Dataset[Int])(implicit mongoConf: MongoConfig, mongoClient: MongoClient): Unit = {

    import users.sparkSession.implicits._

    val userProductsJoin = users.crossJoin(products)

    val userRating = userProductsJoin.map { row => (row.getAs[Int](0), row.getAs[Int](1)) }.rdd

    object RatingOrder extends Ordering[Rating] {
      def compare(x: Rating, y: Rating) = y.rating compare x.rating
    }

    val recommendations = model.predict(userRating)
      .filter(_.rating > 0)
      .groupBy(p => p.user)
      .map{ case (uid, predictions) =>
        val recommendations = predictions.toSeq.sorted(RatingOrder)
          .take(MAX_RECOMMENDATIONS)
          .map(p => Recommendation(p.product, p.rating))

        UserRecommendation(uid, recommendations)
      }.toDF()


    mongoClient(mongoConf.db)(USER_RECS_COLLECTION_NAME).dropCollection()

    recommendations
      .write
      .option("uri", mongoConf.uri)
      .option("collection", USER_RECS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save

    mongoClient(mongoConf.db)(USER_RECS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
  }



  private def calculateProductRecs(maxRecs: Int, model: MatrixFactorizationModel, products: Dataset[Int])(implicit mongoConf: MongoConfig, mongoClient: MongoClient): Unit = {

    import products.sparkSession.implicits._

    object RatingOrder extends Ordering[(Int, Int, Double)] {
      def compare(x: (Int, Int, Double), y: (Int, Int, Double)) = y._3 compare x._3
    }

    val productsVectorRdd = model.productFeatures
      .map{case (movieId, factor) =>
        val factorVector = new DoubleMatrix(factor)
        (movieId, factorVector)
      }

    val minSimilarity = 0.6

    val movieRecommendation = productsVectorRdd.cartesian(productsVectorRdd)
      .filter{ case ((movieId1, vector1), (movieId2, vector2)) => movieId1 != movieId2 }
      .map{case ((movieId1, vector1), (movieId2, vector2)) =>
        val sim = cosineSimilarity(vector1, vector2)
        (movieId1, movieId2, sim)
      }.filter(_._3 >= minSimilarity)
      .groupBy(p => p._1)
      .map{ case (mid:Int, predictions:Iterable[(Int,Int,Double)]) =>
        val recommendations = predictions.toSeq.sorted(RatingOrder)
          .take(maxRecs)
          .map(p => Recommendation(p._2, p._3.toDouble))
        MovieRecommendation(mid, recommendations)
      }.toDF()

    mongoClient(mongoConf.db)(MOVIE_RECS_COLLECTION_NAME).dropCollection()

    movieRecommendation.write
      .option("uri", mongoConf.uri)
      .option("collection", MOVIE_RECS_COLLECTION_NAME)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save

    mongoClient(mongoConf.db)(MOVIE_RECS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))

  }

  /**
    * 余弦相似度
    * @param vec1
    * @param vec2
    * @return
    */
  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}
