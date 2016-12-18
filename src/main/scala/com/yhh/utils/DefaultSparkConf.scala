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
  The first thing a Spark program must do is to create a SparkContext object, which tells Spark how to access a cluster.
  To create a SparkContext you first need to build a SparkConf object that contains information about your application.
  Only one SparkContext may be active per JVM. You must stop() the active SparkContext before creating a new one.
   */
  val sc = new SparkContext(conf)

  //gets a existing sparkSession or, if there is no existing one,creates a new one.
  val spark = SparkSession
    .builder()
    .getOrCreate()

}
