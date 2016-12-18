package com.yhh.examples

import com.yhh.utils.DefaultSparkConf
import org.apache.spark.sql.SaveMode

/**
  * Created by yanghaihui on 12/16/16.
  */
object ReadAndWriteFromDifferentHost extends DefaultSparkConf {

  //Assuming that ks1.tb1's schema equals ks2.tb2's;
  //If not, you should make some conversions.
  def Cassandra1ToCassandra2(): Unit = {
    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("spark.cassandra.connection.host" -> "host1", "keyspace" -> "ks1", "table" -> "tb1"))
      .load()
      .createTempView("tb1")

    val df = spark.sql("select * from tb1")
    df.show(20)

    df.write.format("org.apache.spark.sql.cassandra")
      .options(Map("spark.cassandra.connection.host" -> "host2", "keyspace" -> "tb2", "table" -> "tb2"))
      .mode(SaveMode.Append).save()
  }

  //Assuming that database.table's schema equals ks1.tb1's;
  //If not, you should make some conversions.
  def MysqlToCassandra(): Unit = {
    val mysqlDF = spark.read.format("jdbc")
      .options(Map("driver" -> "com.mysql.jdbc.Driver", "url" -> "jdbc:mysql://host1:3306/database?useSSL=false&serverTimezone=UTC", "dbTable" -> "table", "user" -> "root", "password" -> "root"))
      .load()

    mysqlDF.show(20)

    mysqlDF.write.format("org.apache.spark.sql.cassandra")
      .options(Map("spark.cassandra.connection.host" -> "host2", "keyspace" -> "ks1", "table" -> "tb1"))
      .mode(SaveMode.Append)
      .save()
  }

}
