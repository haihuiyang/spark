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

  /*
  SparkContext是开发Spark应用的入口，它负责和整个集群的交互，包括创建RDD，accumulators and broadcast variables。
  Spark默认的构造函数接受org.apache.spark.SparkConf， 通过这个参数我们可以自定义本次提交的参数，这个参数会覆盖系统的默认配置。
   */
  val sc = new SparkContext(conf)

  //gets a existing sparkSession or, if there is no existing one,creates a new one.
  val spark = SparkSession
    .builder()
    .getOrCreate()

}
