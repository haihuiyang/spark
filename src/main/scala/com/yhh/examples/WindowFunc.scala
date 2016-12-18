package com.yhh.examples

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by yanghaihui on 12/18/16.
  */
class WindowFunc {

  val log = LoggerFactory.getLogger(classOf[WindowFunc])

  /*
      CREATE TABLE test.product (
      product text,
        sale_date text,
        price decimal,
        count int,
        primary key(product,sale_date)
      );

      INSERT INTO product (product, sale_date, price, count ) VALUES ('pencil','2016-01-01',1.2,5);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('pencil','2016-01-02',2.1,4);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('pencil','2016-01-03',3.8,1);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('pencil','2016-01-04',4.3,0);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('pencil','2016-01-05',1.4,8);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('pen','2016-01-01',11.2,5);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('pen','2016-01-02',22.1,3);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('pen','2016-01-03',13.8,2);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('refill','2016-01-01',0.3,15);
      INSERT INTO product (product, sale_date, price, count ) VALUES ('refill','2016-01-02',0.8,9);
   */

  def windowFunc(spark: SparkSession, sc: SparkContext): Unit = {

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.{collect_list, first, last}

    val wSpec = Window.partitionBy("product").orderBy("sale_date").rowsBetween(-1, 0)

    spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> "test",
        "table" -> "product"))
      .load().createTempView("product")

    val product = spark.sql("select product, sale_date, price,count from product")

    product.withColumn("current_price_divide_pre_price", last(product("price")).over(wSpec) divide first(product("price")).over(wSpec))

    product.show(20)
    /*
        +-------+----------+--------------------+-----+
        |product| sale_date|               price|count|
        +-------+----------+--------------------+-----+
        | pencil|2016-01-01|1.200000000000000000|    5|
        | pencil|2016-01-02|2.100000000000000000|    4|
        | pencil|2016-01-03|3.800000000000000000|    1|
        | pencil|2016-01-04|4.300000000000000000|    0|
        | pencil|2016-01-05|1.400000000000000000|    8|
        |    pen|2016-01-01|11.20000000000000...|    5|
        |    pen|2016-01-02|22.10000000000000...|    3|
        |    pen|2016-01-03|13.80000000000000...|    2|
        | refill|2016-01-01|0.300000000000000000|   15|
        | refill|2016-01-02|0.800000000000000000|    9|
        +-------+----------+--------------------+-----+
     */

    log.debug("Calculation completed.")

    val wSpec_10 = Window.partitionBy("product").orderBy("sale_date").rowsBetween(-9, 0)

    val product_count = product.withColumn("ten_days_count_list", collect_list(product("count")).over(wSpec_10))
    product_count.show(20)
    /*
        +-------+----------+--------------------+-----+-------------------+
        |product| sale_date|               price|count|ten_days_count_list|
        +-------+----------+--------------------+-----+-------------------+
        |    pen|2016-01-01|11.20000000000000...|    5|                [5]|
        |    pen|2016-01-02|22.10000000000000...|    3|             [5, 3]|
        |    pen|2016-01-03|13.80000000000000...|    2|          [5, 3, 2]|
        | pencil|2016-01-01|1.200000000000000000|    5|                [5]|
        | pencil|2016-01-02|2.100000000000000000|    4|             [5, 4]|
        | pencil|2016-01-03|3.800000000000000000|    1|          [5, 4, 1]|
        | pencil|2016-01-04|4.300000000000000000|    0|       [5, 4, 1, 0]|
        | pencil|2016-01-05|1.400000000000000000|    8|    [5, 4, 1, 0, 8]|
        | refill|2016-01-01|0.300000000000000000|   15|               [15]|
        | refill|2016-01-02|0.800000000000000000|    9|            [15, 9]|
        +-------+----------+--------------------+-----+-------------------+
     */

    product_count.createTempView("product_with_ten_days_count")

    spark.udf.register("calculate_sum", (valueList: mutable.WrappedArray[Int]) => {
      var sum = 0.0
      valueList.foreach(value => sum += value)
      sum
    })

    val product_count_sum = spark.sql("select product, sale_date, price, calculate_sum(ten_days_count_list) as count_sum from product_with_ten_days_count")

    product_count_sum.show(20)
    /*
        +-------+----------+--------------------+---------+
        |product| sale_date|               price|count_sum|
        +-------+----------+--------------------+---------+
        |    pen|2016-01-01|11.20000000000000...|      5.0|
        |    pen|2016-01-02|22.10000000000000...|      8.0|
        |    pen|2016-01-03|13.80000000000000...|     10.0|
        | pencil|2016-01-01|1.200000000000000000|      5.0|
        | pencil|2016-01-02|2.100000000000000000|      9.0|
        | pencil|2016-01-03|3.800000000000000000|     10.0|
        | pencil|2016-01-04|4.300000000000000000|     10.0|
        | pencil|2016-01-05|1.400000000000000000|     18.0|
        | refill|2016-01-01|0.300000000000000000|     15.0|
        | refill|2016-01-02|0.800000000000000000|     24.0|
        +-------+----------+--------------------+---------+
     */

  }
}
