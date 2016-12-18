package com.yhh.examples

import java.util.UUID

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by yanghaihui on 12/18/16.
  */
abstract class AbstractReadAndSaveUDTValue[T] extends Serializable {

  val log = LoggerFactory.getLogger(classOf[AbstractReadAndSaveUDTValue[T]])

  /*
  CREATE TABLE test.people_with_street (
    id uuid PRIMARY KEY,
    address_with_street frozen<address_with_street>,
    age int,
    name text
  );
  CREATE TYPE test.address_with_street (
      city text,
      street text,
      number int
  );

  CREATE TABLE test.people_no_street (
    id uuid PRIMARY KEY,
    address_no_street frozen<address_no_street>,
    age int,
    name text
  );
  CREATE TYPE test.address_no_street (
      city text,
      number int
  );
   */

  def readUDTValueAndSave(spark: SparkSession, sc: SparkContext): Unit = {
    val peopleWithStreet = sc.cassandraTable("test", "people_with_street").collect()

    val peopleNoStreetListBuffer: mutable.ListBuffer[T] = mutable.ListBuffer.empty

    peopleWithStreet.foreach(row => {
      val id = row.getUUID("id")
      val addressWithStreet: UDTValue = row.getUDTValue("address_with_street")
      val age = row.getInt("age")
      val name = row.getString("name")

      val elem: T = generateData(id, addressWithStreet, age, name)

      peopleNoStreetListBuffer.append(elem)

      log.debug(String.format("Add a elem to peopleNoStreetListBuffer, id : %s, name : %s, age : %s, city : %s, street : %s, number : %s."
        , id.toString, name, age.toString,
        addressWithStreet.getString("city"),
        addressWithStreet.getString("street"),
        addressWithStreet.getInt("number").toString))
    })

    saveToCassandra(peopleNoStreetListBuffer, sc)
  }

  def generateData(id: UUID, addressWithStreet: UDTValue, age: Integer, name: String): T

  def saveToCassandra(data: mutable.ListBuffer[T], sc: SparkContext): Unit
}