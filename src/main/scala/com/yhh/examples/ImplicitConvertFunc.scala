package com.yhh.examples

import org.slf4j.LoggerFactory

/**
  * Created by yanghaihui on 11/25/16.
  */
class ImplicitConvertFunc(arg1: Int, arg2: String) {

  val log = LoggerFactory.getLogger(classOf[ImplicitConvertFunc])

  def main(args: Array[String]): Unit = {
    val people1 = People("Tom", 20)
    val people2 = People("Mike", 40)

    import ImplicitConvertFunc.implicitFunc
    val olderPeople = if (people1.compare(people2) > 0) people1 else people2

    log.debug("%s is order.", olderPeople.name)
  }
}

object ImplicitConvertFunc {
  implicit def implicitFunc(people: People): Ordered[People] = {
    new Ordered[People] {
      override def compare(that: People): Int = {
        people.age.compareTo(that.age)
      }
    }
  }
}

case class People(name: String, age: Integer)