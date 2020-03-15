package com.atguigu.offline

/**
  * 推荐项目
  * @param rid  项目ID
  * @param r    推荐分数
  */
case class Recommendation(rid: Int, r: Double)

/**
  * 电影相似推荐
  * @param mid 电影ID
  * @param recs 相似的电影集合
  */
case class MovieRecommendation(mid: Int, recs: Seq[Recommendation])

/**
  * 用户的电影推荐
  * @param uid 用户ID
  * @param recs 用户的推荐电影集合
  */
case class UserRecommendation(uid: Int, recs: Seq[Recommendation])

case class MongoConfig(val uri: String, val db: String)