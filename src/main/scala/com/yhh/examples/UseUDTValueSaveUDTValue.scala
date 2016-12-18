package com.yhh.examples

import java.util.UUID

import com.datastax.spark.connector.UDTValue
import com.yhh.model.PeopleAddressUseUDTValue
import org.apache.spark.SparkContext

import scala.collection.mutable.ListBuffer

/**
  * Created by yanghaihui on 12/18/16.
  */
class UseUDTValueSaveUDTValue extends AbstractReadAndSaveUDTValue[PeopleAddressUseUDTValue] {
  override def generateData(id: UUID, addressWithStreet: UDTValue, age: Integer, name: String): PeopleAddressUseUDTValue = {
    val addressNoStreet = UDTValue.fromMap(Map("city" -> addressWithStreet.getString("city"), "number" -> addressWithStreet.getInt("number")))
    PeopleAddressUseUDTValue(id, addressNoStreet, age, name)
  }

  override def saveToCassandra(data: ListBuffer[PeopleAddressUseUDTValue], sc: SparkContext): Unit = {
    import com.datastax.spark.connector._
    sc.parallelize(data).saveToCassandra("test", "people_no_street")
  }
}
