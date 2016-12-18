package com.yhh.examples

import com.yhh.utils.DefaultSparkConf
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

/**
  * Created by yanghaihui on 10/29/16.
  */
object ReadAndSaveInMysql extends DefaultSparkConf {

  val log = LoggerFactory.getLogger(ReadAndSaveInMysql.getClass)

  def connectMysql(): Unit = {
    log.debug("Begin connect mysql.")

    import spark.sql

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

    val sqlDF2 = sql("select * from test")

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
