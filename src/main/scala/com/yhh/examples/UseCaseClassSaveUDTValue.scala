package com.yhh.examples

import java.util.UUID

import com.datastax.spark.connector.UDTValue
import com.yhh.model.caseClass.{AddressNoStreet, PeopleAddressUseCaseClass}
import org.apache.spark.SparkContext

import scala.collection.mutable

/**
  * Created by yanghaihui on 12/18/16.
  */
class UseCaseClassSaveUDTValue extends AbstractReadAndSaveUDTValue[PeopleAddressUseCaseClass] {

  override def generateData(id: UUID, addressWithStreet: UDTValue, age: Integer, name: String): PeopleAddressUseCaseClass = {
    val addressNoStreet = AddressNoStreet(addressWithStreet.getString("city"), addressWithStreet.getInt("number"))
    PeopleAddressUseCaseClass(id, addressNoStreet, age, name)
  }

  override def saveToCassandra(data: mutable.ListBuffer[PeopleAddressUseCaseClass], sc: SparkContext): Unit = {
    import com.datastax.spark.connector._
    sc.parallelize(data).saveToCassandra("test", "people_no_street")
  }
}