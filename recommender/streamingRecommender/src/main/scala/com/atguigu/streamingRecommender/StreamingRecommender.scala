package com.atguigu.streamingRecommender

import com.mongodb.casbah.Imports.BasicDBObject
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable

object StreamingRecommender {

  val USER_RECENTLY_RATING_COUNT = 50
  val SIMILAR_MOVIE_COUNT = 20
  val MAX_REC_COUNT = 50

  private val minSimilarity = 0.7

  /**
    * 从Redis中获取当前用户最近K次评分，返回 Buffer[(Int,Double)]
    * @param jedis  Redis客户端
    * @param K    获取当前用户K次评分
    * @param userId   用户ID
    * @param movieId   新评分的电影ID
    * @param score    新评分的电影评分
    * @return    最近K次评分的数组，每一项是<movieId, score>
    */
  def getUserRecentRatings(jedis: Jedis, K: Int, userId: Int, movieId: Int, score: Double): Array[(Int, Double)] = {
    jedis.lrange("uid:"+userId.toString,0,K).map{ line =>
      val attr = line.asInstanceOf[String].split(":")
      (attr(0).toInt, attr(1).toDouble)
    }.toArray
  }

  /**
    * 从广播变量中获取movieId最相似的K个电影，并通过MONGODB来过滤掉已被评分的电影
    * @param mostSimilarMovies  电影相对应的相似矩阵
    * @param collectionForRatingRecords   MongDB数据库连接
    * @param movieId   当前评分的MovieID
    * @param userId    当前评分的UserID
    * @param K
    * @return    返回相似的电影ID数组
    */
  def getSimilarMovies(mostSimilarMovies: scala.collection.Map[Int, Array[Int]], collectionForRatingRecords: MongoCollection, movieId: Int, userId: Int, K: Int): Array[Int] = {

    val similarMoviesBeforeFilter = mostSimilarMovies.getOrElse(movieId, Array[Int]())

    val query = MongoDBObject("uid" -> userId)

    val hasRated = collectionForRatingRecords.find(query).toArray.map(_.get("mid").toString.toInt).toSet
    similarMoviesBeforeFilter.filter(hasRated.contains(_) == false)

  }

  /**
    * 从广播变量中获取movieId1与movieId2的相似度，不存在、或movieId1=movieId2视为毫无相似，相似度为0
    * @param simHash
    * @param movieId1
    * @param movieId2
    * @return  movieId1与movieId2的相似度
    */
  def getSimilarityBetween2Movies(simHash: scala.collection.Map[Int, scala.collection.Map[Int, Double]], movieId1: Int, movieId2: Int): Double = {

    val (smallerId, biggerId) = if (movieId1 < movieId2) (movieId1, movieId2) else (movieId2, movieId1)
    if (smallerId == biggerId) {
      return 0.0
    }
    simHash.get(smallerId) match {
      case Some(subSimHash) =>
        subSimHash.get(biggerId) match {
          case Some(sim) => sim
          case None => 0.0
        }
      case None => 0.0
    }
  }

  def log(m: Double): Double = math.log(m) / math.log(2)

  /**
    * 核心算法，计算每个备选电影的预期评分
    * @param simiHash  电影之间的相似度矩阵
    * @param recentRatings  用户最近评分的K个电影集合
    * @param candidateMovies   当前评分的电影的备选电影集合
    * @return 备选电影预计评分的数组，每一项是<movieId, maybe_rate>
    */
  def createUpdatedRatings(simiHash: scala.collection.Map[Int, scala.collection.Map[Int, Double]], recentRatings: Array[(Int, Double)], candidateMovies: Array[Int]): Array[(Int, Double)] = {

    val allSimilars = mutable.ArrayBuffer[(Int, Double)]()

    val increaseCounter = mutable.Map[Int, Int]()
    val reduceCounter = mutable.Map[Int, Int]()

    for (cmovieId <- candidateMovies; (rmovieId, rate) <- recentRatings) {
      val sim = getSimilarityBetween2Movies(simiHash, rmovieId, cmovieId)
      if (sim > minSimilarity) {
        allSimilars += ((cmovieId, sim * rate))
        if (rate >= 3.0) {
          increaseCounter(cmovieId) = increaseCounter.getOrElse(cmovieId, 0) + 1
        } else {
          reduceCounter(cmovieId) = reduceCounter.getOrElse(cmovieId, 0) + 1
        }
      }
    }
    allSimilars.toArray.groupBy{case (movieId, value) => movieId}
      .map{ case (movieId, simArray) =>
        (movieId, simArray.map(_._2).sum / simArray.length + log(increaseCounter.getOrElse[Int](movieId, 1)) - log(reduceCounter.getOrElse[Int](movieId, 1)))
      }.toArray
  }

  /**
    * 将备选电影的预期评分合并后回写到MONGODB中
    * @param collection
    * @param newRecommends
    * @param userId
    * @param startTimeMillis
    * @return
    */
  def updateRecommends2MongoDB(collection: MongoCollection, newRecommends: Array[(Int, Double)], userId: Int, startTimeMillis: Long): Unit = {


    val query =MongoDBObject("uid"-> userId)

    collection.findOne(query) match {
      case None => {
        insertStreamRecs(collection,newRecommends,userId,startTimeMillis)
      }
      case oldRecs=> {
        val oldMap = scala.collection.mutable.Map[Int,Double]()
        val oldrecommends = oldRecs.toArray.apply(0).asInstanceOf[BasicDBObject].getString("recs")
                              .split("\\|")
                              .map{item=> val attr = item.split(","); oldMap += (attr(0).toInt -> attr(1).toDouble)}

        for (item <- newRecommends){
          if(oldMap.contains(item._1.toInt)){
            if(oldMap.get(item._1.toInt).get < item._2.toDouble){
              oldMap.put(item._1.toInt,item._2.toDouble)
            }
          }else{
            oldMap.put(item._1.toInt,item._2.toDouble)
          }
        }

        collection.remove(query)

        val newArray = oldMap.toList.sortWith(_._2 < _._2 ).slice(0,MAX_REC_COUNT).toArray

        insertStreamRecs(collection,newArray,userId,startTimeMillis)

      }
    }
  }

  def insertStreamRecs(collection: MongoCollection, newRecommends: Array[(Int, Double)], userId: Int, startTimeMillis: Long): Unit ={
    val toInsert = MongoDBObject("uid" -> userId,
      "recs" -> newRecommends.map(item => Recommendation(item._1, item._2)),
      "timedelay" -> (System.currentTimeMillis() - startTimeMillis).toDouble / 1000)
    collection.insert(toInsert)
  }

  def createKafkaStream(ssc:StreamingContext, brokers:String, topic:String) = {
    //kafka的地址 val brobrokers = "192.168.56.150:9092,192.168.56.151:9092,192.168.56.152:9092"

    //kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "streamingRecommenderConsumer",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );

    //创建DStream，返回接收到的输入数据
    var stream=KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam))
    stream
  }
  
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val MOVIE_RECS_COLLECTION_NAME = "MovieRecs"

    val sparkConf = new SparkConf().setAppName("streamingRecommendingSystem")
      .setMaster("local[*]")
      .set("spark.executor.memory", "4g")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))


    import spark.implicits._



    val movieRecs = spark.read
      .option("uri", "mongodb://linux:27017/recommender")
      .option("collection", MOVIE_RECS_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .select($"mid",$"recs")
      .as[MovieRecommendation]
      .map{rec=> (rec.mid, rec.recs.map(x=>(x.rid,x.r)).toMap) }
      .rdd.cache()


    //val movieRecs = spark.read.option("uri", "mongodb://linux:27017/recommender").option("collection", "MovieRecs").format("com.mongodb.spark.sql").load().select($"mid",$"recs").as[MovieRecommendation].map{rec=> (rec.mid, rec.recs.map(x=>(x.rid,x.r)).toMap) }.rdd.cache()

    //每个电影与其他电影的相似度，HASH[电影Id, HASH[电影Id2, Id1与Id2相似度]]
    val movie2movieSimilarity = movieRecs.collectAsMap()

    //每个电影的最相似的K个电影，HASH[电影Id, 相似的K个电影Ids]
    val topKMostSimilarMovies = movieRecs.map(rec=>(rec._1, rec._2.map(_._1).toArray)).collectAsMap()

    /*val topKMostSimilarMovies = sc.textFile(hdfsDir + "simTopK.txt")
      .map{line =>
        val dataArr = line.trim.split(":")
        val movieId = dataArr(0).toInt
        val topK = dataArr(1).split(",").map(_.toInt)
        (movieId, topK)
      }.collectAsMap*/

    //每个电影与其他电影的相似度，HASH[电影Id, HASH[电影Id2, Id1与Id2相似度]]
    /*val movie2movieSimilarity = sc.textFile(hdfsDir + "simSimi.txt")
      .map{line =>
        val dataArr = line.trim.split(":")
        val movieId1 = dataArr(0).toInt
        val similarities = dataArr(1).split(" ").map{str => {
          val similarityArray = str.split(",")
          val movieId2 = similarityArray(0).toInt
          val sim = similarityArray(1).toDouble
          (movieId2, sim)
        }}
        (movieId1, similarities.toMap)
      }.collectAsMap*/



    //最相似电影HASH的广播
    val bTopKMostSimilarMovies = ssc.sparkContext.broadcast(topKMostSimilarMovies)

    //电影间相似度HASH的广播
    val bMovie2movieSimilarity = ssc.sparkContext.broadcast(movie2movieSimilarity)

    //为了在核心任务执行前将广播变量提前pull到各个worker，所以这里做了一堆故意引用了广播的任务
    val firstRDD = ssc.sparkContext.parallelize(1 to 10000, 1000)
    val useless = firstRDD.map{ i =>
      val pullTopK = bTopKMostSimilarMovies.value.contains(i)
      val pullSim = bMovie2movieSimilarity.value.contains(i)
      (i, pullTopK, pullSim)
    }.count()
    println(useless)


    //kafka的地址
    val brobrokers = "192.168.56.150:9092"
    //kafka的队列名称
    val sourcetopic="recommender";

    //kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers,//用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "recommender-consumer-group",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    );

    //创建DStream，返回接收到的输入数据
    var unifiedStream=KafkaUtils.createDirectStream[String,String](ssc, LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(sourcetopic),kafkaParam))

    //单例类获取连接，逃避序列化问题，且一个JVM只有一个连接者，提高性能
    object SingleConnectionClient extends Serializable {
      lazy val mongoClient = MongoClient(MongoClientURI("mongodb://linux:27017/recommender"))
      lazy val jedis = new Jedis("linux")
      def getMongoDBCollection(collectionName: String): MongoCollection = {
        mongoClient("recommender")(collectionName)
      }
      def getRedisClient():Jedis = {
        jedis
      }
    }

    /*val dataDStreams = (1 to 5).map{i =>
      KafkaUtils.createStream(ssc, zkServers, msgConsumerGroup, Map("netflix-recommending-system-topic" -> 3), StorageLevel.MEMORY_ONLY)}

    var unifiedStream = ssc.union(dataDStreams).map(_._2)
    unifiedStream = unifiedStream.repartition(160)*/

    val dataDStream = unifiedStream.map{ case msg =>
      val dataArr: Array[String] = msg.value().split("\\|")
      val userId = dataArr(0).toInt
      val movieId = dataArr(1).toInt
      val score = dataArr(2).toDouble
      val timestamp = dataArr(3).toLong
      (userId, movieId, score, timestamp)
    }.cache

    dataDStream.foreachRDD(rdd => {
      if (!rdd.isEmpty) {
        rdd.map{ case (userId, movieId, score, timestamp) =>
          //从Redis中获取当前用户近期K次评分记录
          val recentRatings = getUserRecentRatings(SingleConnectionClient.getRedisClient(), USER_RECENTLY_RATING_COUNT, userId, movieId, score)

          //获取当前评分电影相似的K个备选电影
          val candidateMovies = getSimilarMovies(bTopKMostSimilarMovies.value, SingleConnectionClient.getMongoDBCollection("Rating"), movieId, userId, SIMILAR_MOVIE_COUNT)

          //为备选电影推测评分结果
          val updatedRecommends = createUpdatedRatings(bMovie2movieSimilarity.value, recentRatings, candidateMovies)

          //当前推荐与往期推荐进行Merge结果回写到MONGODB
          updateRecommends2MongoDB(SingleConnectionClient.getMongoDBCollection("StreamRecs"), updatedRecommends, userId, timestamp)
        }.count()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

