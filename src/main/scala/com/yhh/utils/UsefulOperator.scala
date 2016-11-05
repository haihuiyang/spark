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
    firstRow.size // 2

    firstRow.getInt("count") // 20
    firstRow.getLong("count") // 20L

    firstRow.get[Int]("count") // 20
    firstRow.get[Long]("count") // 20L
    firstRow.get[BigInt]("count") // BigInt(20)
    firstRow.get[java.math.BigInteger]("count") // BigInteger(20)

    firstRow.getIntOption("count") // Some(20)
    firstRow.get[Option[Int]]("count") // Some(20)


    val row = sc.cassandraTable("test", "users").first
    // row: com.datastax.spark.connector.rdd.reader.CassandraRow = CassandraRow{username: someone, emails: [someone@email.com, s@email.com]}

    row.getList[String]("emails") // Vector(someone@email.com, s@email.com)
    row.get[List[String]]("emails") // List(someone@email.com, s@email.com)
    row.get[Seq[String]]("emails") // List(someone@email.com, s@email.com)   :Seq[String]
    row.get[IndexedSeq[String]]("emails") // Vector(someone@email.com, s@email.com) :IndexedSeq[String]
    row.get[Set[String]]("emails") // Set(someone@email.com, s@email.com)

    row.get[String]("emails") // "[someone@email.com, s@email.com]"

    //    CREATE TYPE test.address (city text, street text, number int);
    //    CREATE TABLE test.companies (name text PRIMARY KEY, address FROZEN<address>);

    val address: UDTValue = row.getUDTValue("address")
    val city = address.getString("city")
    val street = address.getString("street")
    val number = address.getInt("number")


    // rdd: com.datastax.spark.connector.rdd.CassandraRDD[com.datastax.spark.connector.rdd.reader.CassandraRow] = CassandraRDD[0] at RDD at CassandraRDD.scala:41

    // rdd.toArray.foreach(println)
    // CassandraRow{word: bar, count: 20}
    // CassandraRow{word: foo, count: 20}


    // val rdd = sc.cassandraTable[SomeType]("ks", "not_existing_table")
    // val emptyRDD = rdd.toEmptyCassandraRDD

    // val emptyRDD2 = sc.emptyCassandraTable[SomeType]("ks", "not_existing_table"))
  }

}
