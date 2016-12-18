package com.yhh.examples

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Created by yanghaihui on 10/29/16.
  */
class ReadAndSaveInCassandra {

  val log = LoggerFactory.getLogger(classOf[ReadAndSaveInCassandra])

  def readData(spark: SparkSession, sc: SparkContext): Unit = {

    log.debug("Begin to read data from cassandra.")

    val load: DataFrame = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "spark.cassandra.connection.host" -> "localhost",
        "spark.cleaner.ttl" -> "3600",
        "keyspace" -> "test",
        "table" -> "words"
      )).load()

    load.createTempView("words")

    val wordsDF = spark.sql("select word,count from words")

    wordsDF.show(20)
    log.debug("Get data from cassandra table.")

  }

  def saveData(spark: SparkSession, sc: SparkContext): Unit = {
    import com.datastax.spark.connector._

    // note: only one sparkContext exists!!!
    sc.stop()

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("Save data example.")
      .set("spark.cassandra.connection.host", "localhost")
    val sc2 = new SparkContext(conf)

    val words1 = sc2.parallelize(Seq(("cat", 30), ("dog", 20), ("fish", 10)))
    words1.saveToCassandra("test", "words", SomeColumns("word", "count"))

    val words2 = sc2.parallelize(Seq(WordCount("aaa", 2), WordCount("bbb", 3)))
    words2.saveToCassandra("test", "words", SomeColumns("word", "count"))

    val allWords = sc2.cassandraTable[(String, Int)]("test", "words").collect()
    allWords.foreach(println)
  }

  case class WordCount(word: String, count: Int)

}