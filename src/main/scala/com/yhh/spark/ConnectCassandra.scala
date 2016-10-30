package com.yhh.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

/**
  * Created by yanghaihui on 10/29/16.
  */
object ConnectCassandra {
  def main(args: Array[String]): Unit = {
    //    ConnectCassandra.readData()
    ConnectCassandra.saveData()
  }


  def readData(): Unit = {
    println("cassandra example: ")

    SparkContext
    val spark = SparkSession
      .builder()
      .appName("connect cassandra example")
      //    .config("spark.some.config.option", "some-value")
      //    .config("spark.cassandra.connection.host", "localhost")
      .master("local[2]")
      .appName("cassandraTest")
      .getOrCreate()


    //    val load: DataFrame = spark.read.format("org.apache.spark.sql.cassandra")
    //      .option("spark.some.config.option", "some-value")
    //      .option("spark.cassandra.connection.host", "localhost")
    //      .option("table", "idx_weight")
    //      .option("keyspace", "gta")
    //      .load()

    val load: DataFrame = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "spark.some.config.option" -> "some-value",
        "spark.cassandra.connection.host" -> "localhost",
        "table" -> "words",
        "spark.cleaner.ttl" -> "3600",
        "keyspace" -> "test"
      )).load()

    load.createOrReplaceTempView("words")

    //    val sqlDF1 = sql("select distinct(symbol),sampleSecurityCode,tradingDate, WEIGHT from idx_weight where " +
    //      "sampleSecurityCode >= \'600004\' order by weight desc")

    import spark.implicits._
    val sqlDF1 = spark.sql("select word,count from words")

    sqlDF1.map({
      case Row(key: String, value: Int) => s"Key: $key, Value: $value"
    }).show(10)


    sqlDF1.show(20)
    println("get data from cassandra db.")

  }

  def saveData(): Unit = {
    import com.datastax.spark.connector._
    //Loads implicit functions

    val conf = new SparkConf()
    conf.set("spark.cassandra.connection.host", "localhost")
    conf.setMaster("local[2]")
    conf.set("table", "words")
    conf.set("keyspace", "test")
    conf.setAppName("test2")

    val sc = new SparkContext(conf)

    val collection = sc.parallelize(Seq(("cat", 30), ("dog", 20), ("fish", 10)))
    collection.saveToCassandra("test", "words", SomeColumns("word", "count"))


    val collection1 = sc.parallelize(Seq(WordCount("aaa", 2), WordCount("bbb", 3)))
    collection1.saveToCassandra("test", "words", SomeColumns("word", "count"))

    val results = sc.cassandraTable[(String, Int)]("test", "words").collect()
    results.foreach(println)

  }

  case class WordCount(word: String, count: Int)

}