package com.yhh.examples

import java.util.UUID

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable


/**
  * Created by yanghaihui on 12/7/16.
  */
class TwoWaysToReadAndSaveUDTValue extends Serializable {

  val log = LoggerFactory.getLogger(classOf[TwoWaysToReadAndSaveUDTValue])

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

  def useUDTValueToSave(spark: SparkSession, sc: SparkContext): Unit = {

    val peopleWithStreet = sc.cassandraTable("test", "people_with_street").collect()

    val peopleNoStreetListBuffer: mutable.ListBuffer[PeopleNoStreet] = mutable.ListBuffer.empty

    peopleWithStreet.foreach(row => {
      val id = row.getUUID("id")
      val addressWithStreet: UDTValue = row.getUDTValue("address_with_street")
      val age = row.getInt("age")
      val name = row.getString("name")

      val addressNoStreet = UDTValue.fromMap(Map("city" -> addressWithStreet.getString("city"), "number" -> addressWithStreet.getInt("number")))

      peopleNoStreetListBuffer.append(PeopleNoStreet(id, addressNoStreet, age, name))

      log.debug(String.format("Add a elem to peopleNoStreetListBuffer, id : %s, name : %s, age : %s, city : %s, street : %s, number : %s."
        , id.toString, name, age.toString,
        addressWithStreet.getString("city"),
        addressWithStreet.getString("street"),
        addressWithStreet.getInt("number").toString))
    })

    sc.parallelize(peopleNoStreetListBuffer).saveToCassandra("test", "people_no_street")
  }

  def useCaseClassToSave(spark: SparkSession, sc: SparkContext): Unit = {

    val peopleWithStreet = sc.cassandraTable("test", "people_with_street").collect()

    val peopleNoStreetListBuffer: mutable.ListBuffer[PeopleNoStreetUseCaseClass] = mutable.ListBuffer.empty

    peopleWithStreet.foreach(row => {
      val id = row.getUUID("id")
      val addressWithStreet: UDTValue = row.getUDTValue("address_with_street")
      val age = row.getInt("age")
      val name = row.getString("name")

      val addressNoStreet = AddressNoStreet(addressWithStreet.getString("city"), addressWithStreet.getInt("number"))

      peopleNoStreetListBuffer.append(PeopleNoStreetUseCaseClass(id, addressNoStreet, age, name))

      log.debug(String.format("Add a elem to peopleNoStreetListBuffer, id : %s, name : %s, age : %s, city : %s, street : %s, number : %s."
        , id.toString, name, age.toString,
        addressWithStreet.getString("city"),
        addressWithStreet.getString("street"),
        addressWithStreet.getInt("number").toString))
    })

    sc.parallelize(peopleNoStreetListBuffer).saveToCassandra("test", "people_no_street")
  }

  case class AddressNoStreet(city: String, number: Integer)

  case class PeopleNoStreet(id: UUID, addressNoStreet: UDTValue, age: Integer, name: String)

  case class PeopleNoStreetUseCaseClass(id: UUID, addressNoStreet: AddressNoStreet, age: Integer, name: String)
}