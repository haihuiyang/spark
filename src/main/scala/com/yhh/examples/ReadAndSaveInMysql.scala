package com.yhh.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * Created by yanghaihui on 10/29/16.
  */
class ReadAndSaveInMysql {

  val log = LoggerFactory.getLogger(classOf[ReadAndSaveInMysql])

  def connectMysql(spark: SparkSession, sc: SparkContext): Unit = {
    log.debug("Begin connect mysql.")

    val mysqlDF = spark.read.format("jdbc")
      .options(Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url" -> "jdbc:mysql://host1:3306/database?useSSL=false&serverTimezone=UTC",
        "dbTable" -> "table",
        "user" -> "root",
        "password" -> "root"))
      .load()

    //create a temp view for sql selection.
    mysqlDF.createOrReplaceTempView("test")

    val sqlDF2 = spark.sql("select * from test")

    sqlDF2.show(20)

    sqlDF2.write.format("jdbc")
      .options(Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url" -> "jdbc:mysql://host1:3306/database?useSSL=false&serverTimezone=UTC",
        "dbTable" -> "table",
        "user" -> "root",
        "password" -> "root"))
      .mode(SaveMode.Append)
      .save()

    //stop a sparkContext,and so you can declare another sparkContext.
    sc.stop()
  }
}
