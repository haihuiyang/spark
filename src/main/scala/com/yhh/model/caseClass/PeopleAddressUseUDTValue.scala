package com.yhh.model.caseClass

import java.util.UUID

import com.datastax.spark.connector.UDTValue

/**
  * Created by yanghaihui on 12/18/16.
  */
case class PeopleAddressUseUDTValue(id: UUID, addressNoStreet: UDTValue, age: Integer, name: String) {

}
