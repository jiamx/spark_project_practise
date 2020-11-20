package com.jmx.demos

import com.jmx.metrics.{BestFilmsByOverallRating, GenresByAverageRating, MostRatedFilms}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
  *  @Created with IntelliJ IDEA.
  *  @author : jmx
  *  @Date: 2020/11/19
  *  @Time: 16:27
  *  */
object DemoMainApp {
  // 文件路径
  private val MOVIES_CSV_FILE_PATH = "file:///e:/movies.csv"
  private val RATINGS_CSV_FILE_PATH = "file:///e:/ratings.csv"


  def main(args: Array[String]): Unit = {
    // 创建spark session
    val spark = SparkSession
      .builder
      .master("local[4]")
      .getOrCreate
    // schema信息
    val schemaLoader = new SchemaLoader
    // 读取Movie数据集
    val movieDF = readCsvIntoDataSet(spark, MOVIES_CSV_FILE_PATH, schemaLoader.getMovieSchema)

    // 读取Rating数据集
    val ratingDF = readCsvIntoDataSet(spark, RATINGS_CSV_FILE_PATH, schemaLoader.getRatingSchema)

    movieDF.printSchema()
    ratingDF.printSchema()

    // 需求1：查找电影评分个数超过5000,且平均评分较高的前十部电影名称及其对应的平均评分
    val bestFilmsByOverallRating = new BestFilmsByOverallRating
    //bestFilmsByOverallRating.run(movieDF, ratingDF, spark)

    // 需求2：查找每个电影类别及其对应的平均评分
    val genresByAverageRating = new GenresByAverageRating
    //genresByAverageRating.run(movieDF, ratingDF, spark)

    // 需求3：
    val mostRatedFilms = new MostRatedFilms
    mostRatedFilms.run(movieDF, ratingDF, spark)


    spark.close()

  }

  /**
    * 读取数据文件，转成DataFrame
    *
    * @param spark
    * @param path
    * @param schema
    * @return
    */
  def readCsvIntoDataSet(spark: SparkSession, path: String, schema: StructType) = {

    val dataSet = spark.read
      .format("csv")
      .option("header", "true")
      .schema(schema)
      .load(path)
    dataSet

  }


}
