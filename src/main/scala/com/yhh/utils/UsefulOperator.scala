package com.yhh.utils

/**
  * Created by yanghaihui on 11/5/16.
  */
object UsefulOperator extends MySparkConf {

  def main(args: Array[String]): Unit = {
    println("here is some useful operator in spark and scala.")
    operator1()

  }

  def operator1(): Unit = {
    /*
     table: words
         word | count
        ------+-------
          aaa |     2
          bbb |     3
          cat |    30
          dog |    20
         fish |    10
     */
    import com.datastax.spark.connector._

    val rdd = sc.cassandraTable("test", "words")
    println(rdd.count)
    println(rdd.first)
    println(rdd.map(_.getInt("count")).sum)

    val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
    collection.saveToCassandra("test", "words", SomeColumns("word", "count"))

    /*
    output:
          5
          CassandraRow{word: cat, count: 30}
          65.0
     */
  }

  def operator2(): Unit = {
    //Loads implicit functions
    import com.datastax.spark.connector._
    sc.cassandraTable("keyspace name", "table name")

    val rdd = sc.cassandraTable("test", "words")

    val firstRow = rdd.first
    // firstRow: com.datastax.spark.connector.rdd.reader.CassandraRow = CassandraRow{word: bar, count: 20}

    // firstRow.columnNames // Stream(word, count)
    firstRow.size           // 2

    firstRow.getInt("count")       // 20
    firstRow.getLong("count")      // 20L

    firstRow.get[Int]("count")                   // 20
    firstRow.get[Long]("count")                  // 20L
    firstRow.get[BigInt]("count")                // BigInt(20)
    firstRow.get[java.math.BigInteger]("count")  // BigInteger(20)

    // rdd: com.datastax.spark.connector.rdd.CassandraRDD[com.datastax.spark.connector.rdd.reader.CassandraRow] = CassandraRDD[0] at RDD at CassandraRDD.scala:41

    // rdd.toArray.foreach(println)
    // CassandraRow{word: bar, count: 20}
    // CassandraRow{word: foo, count: 20}


    // val rdd = sc.cassandraTable[SomeType]("ks", "not_existing_table")
    // val emptyRDD = rdd.toEmptyCassandraRDD

    // val emptyRDD2 = sc.emptyCassandraTable[SomeType]("ks", "not_existing_table"))
  }

}
