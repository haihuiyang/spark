package com.yhh.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql._

import org.apache.log4j.Logger


/**
  * Hello world!
  *
  */
object App {
  def main(args: Array[String]): Unit = {

    val log = Logger.getLogger(this.getClass())

    println("cassandra example: ")

    SparkContext
    val spark = SparkSession
      .builder()
      .appName("Spark cassandra Example")
      .config("spark.some.config.option", "some-value")
      .config("spark.cassandra.connection.host", "localhost")
      .master("local[4]")
      .appName("cassandraTest")
      .getOrCreate()

    val load: DataFrame = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "idx_weight", "keyspace" -> "gta")).load()

    log.warn("load idx_weight from gta keyspace.")

    load.createOrReplaceTempView("idx_weight")

    import spark.sql

    val sqlDF1 = sql("select symbol,sampleSecurityCode,tradingDate, WEIGHT from idx_weight where sampleSecurityCode >= \'600004\' and" +
      " sampleSecurityCode <= \'600008\'")

    sqlDF1.show(10)
    println("get data from cassandra db.")

    println("\n\n\n")

    println("sql example: ")

    SQLContext
    val jdbcDF = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC")
      .option("dbTable", "test")
      .option("user", "root")
      .option("password", "root")
      .load()

    jdbcDF.createOrReplaceTempView("idx_weight")

    val sqlDF2 = sql("select * from idx_weight")

    sqlDF2.show(20)
  }

  println("Hello World!")
}
