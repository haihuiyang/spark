package com.yhh.spark

import scala.Predef._

/**
  * Created by yanghaihui on 11/5/16.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val r_recursion = f_recursion("Hello")
    println(r_recursion)
    val r = f("Hello")
    println(r)
    f1()
  }

  //无法声明匿名递归函数？怎么声明
  def f_recursion(s: String): Long = {
    if (s.size == 1) {
      s.takeRight(1).charAt(0).toLong
    } else {
      s.takeRight(1).charAt(0).toLong * f_recursion(s.dropRight(1))
    }
  }

  def f = (s: String) => {
    var t: Long = 1
    for (i <- s) t *= i
    t
  }

  def f1 = () => {
    var first = true
    val a = Array(1, 2, 3, 4, -1, -4, 2, 5, -10)
    a.foreach(i => print(i.toString + " "))
    println()
    println(a.length)
    val indexes = for (i <- 0 until a.length if first || a(i) >= 0) yield {
      if (a(i) < 0) first = false
      i
    }
    println(indexes)
    for (j <- 0 until indexes.length) {
      a(j) = a(indexes(j))
      println(a.mkString(","))
      //      print(a(j) + " ")不支持，不会自动toString，必须手动toString
    }
  }
}
