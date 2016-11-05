package com.yhh.spark

import java.util.UUID

import com.yhh.utils.MySparkConf

/**
  * Created by yanghaihui on 11/5/16.
  */
object TestCassandraOperator extends MySparkConf {
  def main(args: Array[String]): Unit = {
    import com.datastax.spark.connector._

    //  val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
    //  collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
    val rdd = sc.cassandraTable("bar", "factorsystem")

    rdd.select("risk_model_types").foreach(row => println(row.getList[UUID]("risk_model_types").mkString))

  }

}
