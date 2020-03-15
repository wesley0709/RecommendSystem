package com.atguigu.statisticsRecommender

import java.text.SimpleDateFormat
import java.util.Date

import com.atguigu.statisticsRecommender.StatisticsApp.{RATINGS_COLLECTION_NAME, mongoConf, spark}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


// MongoDB Config
case class MongoConfig(val uri: String, val db: String)

// Rating Class
case class Rating(mid: Int, uid: Int, score: Double, timestamp: Long)

case class Recommendation(rid: Int, r: Double)

case class GenresRecommendation(genres: String, recs: Seq[Recommendation])


/**
  * Movie Class 电影类
  *
  * @param mid       电影的ID
  * @param name      电影的名称
  * @param descri    电影的描述
  * @param timelong  电影的时长
  * @param issue     电影的发行时间
  * @param shoot     电影的拍摄时间
  * @param language  电影的语言
  * @param genres    电影的类别
  * @param actors    电影的演员
  * @param directors 电影的导演
  */
case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String, val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)


object statisticsRecommender {

  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_MOVIES_RECENTLY = "RateMoreMoviesRecently"
  val AVERAGE_MOVIES_SCORE = "AverageMoviesScore"
  val GENRES_TOP_MOVIES = "GenresTopMovies"


  /**
    * 评分最多统计
    */
  def rateMore(spark: SparkSession)(implicit mongoConf: MongoConfig): Unit = {

    val rateMoreDF = spark.sql("select mid, count(mid) as count from ratings group by mid order by count desc")

    rateMoreDF
      .write
      .option("uri", mongoConf.uri)
      .option("collection", RATE_MORE_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  /**
    * 最热电影统计
    */
  def rateMoreRecently(spark: SparkSession)(implicit mongoConf: MongoConfig): Unit = {

    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changDate", (x: Long) => simpleDateFormat.format(new Date(x * 1000L)).toLong)

    val yeahMonthOfRatings = spark.sql("select mid, uid, score, changDate(timestamp) as yeahmonth from ratings")

    yeahMonthOfRatings.createOrReplaceTempView("ymRatings")

    val rateMoreRecentlyDF = spark.sql("select mid, count(mid) as count,yeahmonth from ymRatings group by yeahmonth,mid order by yeahmonth desc,count desc")

    rateMoreRecentlyDF
      .write
      .option("uri", mongoConf.uri)
      .option("collection", RATE_MORE_MOVIES_RECENTLY)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  /**
    * 最热电影统计
    */
  def averageMovieScore(spark: SparkSession)(movies: Dataset[Movie])(implicit mongoConf: MongoConfig): Unit = {

    val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
    ,"Romance","Science","Tv","Thriller","War","Western")

    val averageMovieScoreDF = spark.sql("select mid, avg(score) as avg from ratings group by mid").cache()

    // 统计每种类别最热电影【每种类别中平均评分最高的10部电影】
    val moviesWithSocreDF = movies.join(averageMovieScoreDF,Seq("mid","mid")).select("mid","avg","genres").cache()

    val genresRdd = spark.sparkContext.makeRDD(genres);

    import spark.implicits._

    val genresTopMovies = genresRdd.cartesian(moviesWithSocreDF.rdd).filter{
      case (genres,row) => {
        row.getAs[String]("genres").toLowerCase().contains(genres.toLowerCase)
      }
    }.map{
      case (genres,row) => {
        (genres,(row.getAs[Int]("mid"),row.getAs[Double]("avg")))
      }
    }.groupByKey()
        .map{
          case (genres,items) => {
            GenresRecommendation(genres,items.toList.sortWith(_._2 > _._2).slice(0,10).map(x=>Recommendation(x._1,x._2)))
          }
        }.toDF

    genresTopMovies
      .write
      .option("uri", mongoConf.uri)
      .option("collection", GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    averageMovieScoreDF
      .write
      .option("uri", mongoConf.uri)
      .option("collection", AVERAGE_MOVIES_SCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  /**
    * 每种类别最热电影推荐
    */
  def genresHot(spark: SparkSession)(implicit mongoConf: MongoConfig): Unit = {

    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changDate", (x: Long) => simpleDateFormat.format(new Date(x * 1000L)).toLong)

    val yeahMonthOfRatings = spark.sql("select mid, uid, score, changDate(timestamp) as yeahmonth from ratings")

    yeahMonthOfRatings.createOrReplaceTempView("ymRatings")

    val rateMoreRecentlyDF = spark.sql("select mid, count(mid) as count,yeahmonth from ymRatings group by yeahmonth,mid order by yeahmonth desc,count desc")

    rateMoreRecentlyDF
      .write
      .option("uri", mongoConf.uri)
      .option("collection", RATE_MORE_MOVIES_RECENTLY)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}


