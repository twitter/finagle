package com.twitter.finagle.exp.swift

import com.twitter.util.Future
import scala.reflect.BeanProperty
import scala.annotation.target._

@ThriftStruct
case class Test1Exc(
  @ThriftField(1) @BeanProperty var m: String,
  @ThriftField(2) @BeanProperty var howmany: java.lang.Integer
) extends Exception(m) {
  def this() = this("", 0)
}

@ThriftService("Test1")
trait Test1 {
  @ThriftMethod(exception = Array(
    new ThriftException(`type` = classOf[Test1Exc], id=1)))
  def ab(
      a: String, 
      b: java.util.Map[String, java.lang.Integer]
  ): Future[java.util.Map[String, java.lang.Integer]]
  
  @ThriftMethod
  def ping(msg: String): Future[String]
}
