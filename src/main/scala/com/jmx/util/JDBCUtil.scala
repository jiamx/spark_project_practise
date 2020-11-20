package com.jmx.util

import com.jmx.demos.tenGreatestMoviesByAverageRating
import com.mchange.v2.c3p0.ComboPooledDataSource
import org.apache.commons.dbutils.QueryRunner

/**
  *  @Created with IntelliJ IDEA.
  *  @author : jmx
  *  @Date: 2020/11/19
  *  @Time: 17:54
  *  */
object JDBCUtil {
  val dataSource = new ComboPooledDataSource()

  val user = "root"
  val password = "920724"
  val url = "jdbc:mysql://localhost:3306/mydb"

  dataSource.setUser(user)
  dataSource.setPassword(password)
  dataSource.setDriverClass("com.mysql.jdbc.Driver")
  dataSource.setJdbcUrl(url)
  dataSource.setAutoCommitOnClose(false)


  def getQueryRunner(): Option[QueryRunner]={
    try {
      Some(new QueryRunner(dataSource))
    }catch {
      case e:Exception =>
        e.printStackTrace()
        None

    }
  }



}
