package com.yhh.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql._

/**
  * Created by yanghaihui on 10/29/16.
  */
object ConnectMysql {
  def main(args: Array[String]): Unit = {

    //first need a sparkContext
    import org.apache.spark.SparkConf

    //Configuration for a Spark application.
    val conf = new SparkConf()
      .setAppName("Connection mysql db example.")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    //    SparkContext    what is its used?
    //gets a existing sparkSession or, if there is no existing one,creates a new one.
    val spark = SparkSession
      .builder()
      //master and appName can set in sparkContext.
      //      .appName("connect mysql example.")
      //      .master("local[2]")
      .getOrCreate()

    println("sql example: ")

    import spark.sql

    //    SQLContext      what is its used?
    //use spark.read.format() to connect mysql database.
    val jdbcDF = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC")
      .option("dbTable", "test")
      .option("user", "root")
      .option("password", "root")
      .load()

    //create a temp view for sql selection.
    jdbcDF.createOrReplaceTempView("test")

    val sqlDF2 = sql("select * from test")

    sqlDF2.show(20)

    //stop a sparkContext,and so you can declare another sparkContext.
    sc.stop()
  }
}
