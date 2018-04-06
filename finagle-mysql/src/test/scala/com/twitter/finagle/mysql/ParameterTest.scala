package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.Parameter._
import org.scalatest.FunSuite

class ParameterTest extends FunSuite {
  test("Parameter.unsafeWrap(null)") {
    assert(Parameter.unsafeWrap(null) == NullParameter)
  }

  test("String") {
    val value = "yep"
    val param: Parameter = value
    assert(param.value == value)
  }

  test("Boolean") {
    val value = true
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.lang.Boolean") {
    val value = Boolean.box(true)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("Byte") {
    val value = 3.toByte
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.lang.Byte") {
    val value = Byte.box(3)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("Short") {
    val value = 3.toShort
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.lang.Short") {
    val value = Short.box(3)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("Int") {
    val value = 3
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.lang.Int") {
    val value = Int.box(3)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("Long") {
    val value = 3L
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.lang.Long") {
    val value = Long.box(3L)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("BigInt") {
    val value = BigInt(3)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("Float") {
    val value = 3.0F
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.lang.Float") {
    val value = Float.box(3.0F)
    val param: Parameter = value
    assert(param.value == value)
  }


  test("Double") {
    val value = 3.0
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.lang.Double") {
    val value = Double.box(3.0)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("BigDecimal") {
    val value = BigDecimal(3.0)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("Array[Byte]") {
    val value = Array(3.toByte)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.sql.Timestamp") {
    val value = new java.sql.Timestamp(3L)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.sql.Date") {
    val value = new java.sql.Date(3L)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("java.util.Date") {
    val value = new java.util.Date(3L)
    val param: Parameter = value
    assert(param.value == value)
  }

  test("null") {
    val value: String = null
    val param: Parameter = value
    assert(param.value == null)
  }

  test("null on its own is not converted") {
    val param: Parameter = null
    assert(param == null)
  }

  test("Option as Some") {
    val value: Option[String] = Some("yep")
    val param: Parameter = value
    assert(param.value == value.get)
  }

  test("Option as None") {
    val value: Option[String] = None
    val param: Parameter = value
    assert(param.value == null)
  }
}
