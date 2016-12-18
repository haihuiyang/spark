package com.yhh.spark

import com.yhh.examples.WindowFunc
import com.yhh.utils.DefaultSparkConf
import org.apache.log4j.Logger


/**
  * Hello world!
  *
  */
object App extends DefaultSparkConf {

  val log = Logger.getLogger(App.getClass)

  //Unit is same as java type void.
  def main(args: Array[String]): Unit = {
    //    new ReadAndSaveUDTValue().useUDTValueToSave(spark, sc)

    //    new UseCaseClassSaveUDTValue().readUDTValueAndSave(spark, sc)
    //    new UseUDTValueSaveUDTValue().readUDTValueAndSave(spark, sc)
    new WindowFunc().windowFunc(spark, sc)
    sc.stop() //stop sparkContext.
  }
}
