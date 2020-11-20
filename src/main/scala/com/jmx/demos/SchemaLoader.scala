package com.jmx.demos
import org.apache.spark.sql.types.{DataTypes, StructType}
/**
  *  @Created with IntelliJ IDEA.
  *  @author : jmx
  *  @Date: 2020/11/19
  *  @Time: 15:17
  *  */


class SchemaLoader {

  private val movieSchema = new StructType()
    .add("movieId", DataTypes.StringType, false)
    .add("title", DataTypes.StringType, false)
    .add("genres", DataTypes.StringType, false)

  private val ratingSchema = new StructType()
    .add("userId", DataTypes.StringType, false)
    .add("movieId", DataTypes.StringType, false)
    .add("rating", DataTypes.StringType, false)
    .add("timestamp", DataTypes.StringType, false)

  def getMovieSchema: StructType = movieSchema

  def getRatingSchema: StructType = ratingSchema
}