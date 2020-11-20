package com.jmx.metrics

/**
  *  @Created with IntelliJ IDEA.
  *  @author : jmx
  *  @Date: 2020/11/19
  *  @Time: 15:21
  *  */

import com.jmx.demos.topGenresByAverageRating
import com.jmx.util.JDBCUtil
import org.apache.commons.dbutils.QueryRunner
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 需求2：查找每个电影类别及其对应的平均评分
  */
class GenresByAverageRating extends Serializable {
  def run(moviesDataset: DataFrame, ratingsDataset: DataFrame, spark: SparkSession) = {
    import spark.implicits._
    // 将moviesDataset注册成表
    moviesDataset.createOrReplaceTempView("movies")
    // 将ratingsDataset注册成表
    ratingsDataset.createOrReplaceTempView("ratings")

    val ressql2 =
      """
        |WITH explode_movies AS (
        |SELECT
        |	movieId,
        |	title,
        |	category
        |FROM
        |	movies lateral VIEW explode ( split ( genres, "\\|" ) ) temp AS category
        |)
        |SELECT
        |	m.category AS genres,
        |	avg( r.rating ) AS avgRating
        |FROM
        |	explode_movies m
        |	JOIN ratings r ON m.movieId = r.movieId
        |GROUP BY
        |	m.category
        | """.stripMargin

    val resultDS = spark.sql(ressql2).as[topGenresByAverageRating]

    // 打印数据
    resultDS.show(10)
    resultDS.printSchema()
    // 写入MySQL
    resultDS.foreachPartition(par => par.foreach(insert2Mysql(_)))


  }

  /**
    * 获取连接，调用写入MySQL数据的方法
    *
    * @param res
    */
  private def insert2Mysql(res: topGenresByAverageRating): Unit = {
    lazy val conn = JDBCUtil.getQueryRunner()
    conn match {
      case Some(connection) => {
        upsert(res, connection)
      }
      case None => {
        println("Mysql连接失败")
        System.exit(-1)
      }
    }
  }

  /**
    * 封装将结果写入MySQL的方法
    * 执行写入操作
    *
    * @param r
    * @param conn
    */
  private def upsert(r: topGenresByAverageRating, conn: QueryRunner): Unit = {
    try {
      val sql =
        s"""
           |REPLACE INTO `genres_average_rating`(
           |genres,
           |avgRating
           |)
           |VALUES
           |(?,?)
       """.stripMargin
      // 执行insert操作
      conn.update(
        sql,
        r.genres,
        r.avgRating
      )
    } catch {
      case e: Exception => {
        e.printStackTrace()
        System.exit(-1)
      }
    }
  }


}
