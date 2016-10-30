package com.yhh.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  * Created by yanghaihui on 10/29/16.
  */
object ConnectMysql {
  def main(args: Array[String]): Unit = {
    SparkContext
    val spark = SparkSession
      .builder()
      .appName("connect mysql example.")
      .master("local[2]")
      .getOrCreate()

    println("sql example: ")

    import spark.sql

    SQLContext
    val jdbcDF = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC")
      .option("dbTable", "test")
      .option("user", "root")
      .option("password", "root")
      .load()

    jdbcDF.createOrReplaceTempView("test")

    val sqlDF2 = sql("select * from test")

    sqlDF2.show(20)
  }
}
