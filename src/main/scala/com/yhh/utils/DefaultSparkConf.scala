package com.yhh.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yanghaihui on 11/5/16.
  */
trait DefaultSparkConf {

  val conf = new SparkConf()
    .setAppName("Spark test example.")
    .setMaster("local")
    .set("spark.cassandra.connection.host", "localhost")

  val sc = new SparkContext(conf)

  val spark = SparkSession
    .builder()
    .getOrCreate()

}
