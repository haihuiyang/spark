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

    import org.apache.spark.SparkConf

    // you don't have to declare a sparkContext, it's not required.
    val conf = new SparkConf()
      .setMaster("local[2]")
      .set("spark.cassandra.connection.host", "localhost")
      .set("spark.some.config.option", "some-value")
      .setAppName("example app.")

    val sc = new SparkContext(conf)

    //    SparkContext
    val spark = SparkSession
      .builder()
      //      .appName("Spark cassandra Example")
      //      .config("spark.some.config.option", "some-value")
      //      .config("spark.cassandra.connection.host", "localhost")
      //      .config("spark.cassandra.connection.host", ""127.0.0.1,127.0.0.2"")
      //      .master("local[4]")
      //      .master("spark://master:7077")
      //      .appName("cassandraTest")
      .getOrCreate()

    val load: DataFrame = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "idx_weight", "keyspace" -> "gta")).load()

    log.info("load idx_weight from gta keyspace.")

    load.createOrReplaceTempView("idx_weight")

    import spark.sql

    val sqlDF1 = sql("select symbol,sampleSecurityCode,tradingDate, WEIGHT from idx_weight where sampleSecurityCode >= \'600004\' and" +
      " sampleSecurityCode <= \'600008\' and weight > 0.5")

    sqlDF1.show(10)

    /*
    window functions.
     */
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val wSpec = Window.partitionBy("symbol").orderBy("tradingDate").rowsBetween(Long.MinValue, 1)

    sqlDF1.withColumn("avg", sum(sqlDF1("WEIGHT")).over(wSpec)).show()


    println("get data from cassandra db.")

    println("\n\n\n")

    println("sql example: ")

    //    SQLContext
    val loadSQL: DataFrame = spark.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC")
      .option("dbTable", "test")
      .option("user", "root")
      .option("password", "root")
      .load()

    loadSQL.createOrReplaceTempView("idx_weight")
    loadSQL.printSchema()

    val sqlDF2 = sql("select * from idx_weight")

    sqlDF2.show(20)

    sc.stop() //stop sparkContext.
  }

  println("Hello World!")
}
