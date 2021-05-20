package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.Parameter._
import com.twitter.finagle.mysql.transport.MysqlBuf
import org.scalatest.funsuite.AnyFunSuite

class ParameterTest extends AnyFunSuite {
  test("Parameter.unsafeWrap(null)") {
    assert(Parameter.unsafeWrap(null) == NullParameter)
  }

  test("String") {
    val value = "yep"
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.stringCanBeParameter)
    assert(param.value == value)
  }

  test("Boolean") {
    val value = true
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.booleanCanBeParameter)
    assert(param.value == value)
  }

  test("java.lang.Boolean") {
    val value = Boolean.box(true)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.javaLangBooleanCanBeParameter)
    assert(param.value == value)
  }

  test("Byte") {
    val value = 3.toByte
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.byteCanBeParameter)
    assert(param.value == value)
  }

  test("java.lang.Byte") {
    val value = Byte.box(3)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.javaLangByteCanBeParameter)
    assert(param.value == value)
  }

  test("Short") {
    val value = 3.toShort
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.shortCanBeParameter)
    assert(param.value == value)
  }

  test("java.lang.Short") {
    val value = Short.box(3)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.javaLangShortCanBeParameter)
    assert(param.value == value)
  }

  test("Int") {
    val value = 3
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.intCanBeParameter)
    assert(param.value == value)
  }

  test("java.lang.Int") {
    val value = Int.box(3)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.javaLangIntCanBeParameter)
    assert(param.value == value)
  }

  test("Long") {
    val value = 3L
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.longCanBeParameter)
    assert(param.value == value)
  }

  test("java.lang.Long") {
    val value = Long.box(3L)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.javaLangLongCanBeParameter)
    assert(param.value == value)
  }

  test("BigInt") {
    val value = BigInt(3)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.bigIntCanBeParameter)
    assert(param.value == value)
  }

  test("Float") {
    val value = 3.0f
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.floatCanBeParameter)
    assert(param.value == value)
  }

  test("java.lang.Float") {
    val value = Float.box(3.0f)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.javaLangFloatCanBeParameter)
    assert(param.value == value)
  }

  test("Double") {
    val value = 3.0
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.doubleCanBeParameter)
    assert(param.value == value)
  }

  test("java.lang.Double") {
    val value = Double.box(3.0)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.javaLangDoubleCanBeParameter)
    assert(param.value == value)
  }

  test("BigDecimal") {
    val value = BigDecimal(3.0)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.bigDecimalCanBeParameter)
    assert(param.value == value)
  }

  test("Array[Byte]") {
    val value = Array(3.toByte)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.byteArrayCanBeParameter)
    assert(param.value == value)
  }

  test("java.sql.Timestamp") {
    val value = new java.sql.Timestamp(3L)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.dateCanBeParameter)
    assert(param.typeCode == Type.Timestamp)
    assert(param.value == value)
  }

  test("java.sql.Date") {
    val value = new java.sql.Date(3L)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.dateCanBeParameter)
    assert(param.typeCode == Type.Date)
    assert(param.value == value)
  }

  test("java.util.Date") {
    val value = new java.util.Date(3L)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.dateCanBeParameter)
    assert(param.typeCode == Type.DateTime)
    assert(param.value == value)
  }

  test("com.twitter.util.Time") {
    val value = com.twitter.util.Time.fromMilliseconds(3L)
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.ctuTimeCanBeParameter)
    assert(param.value == value)
  }

  test("null") {
    val value: String = null
    val param: Parameter = value
    assert(param.evidence == CanBeParameter.nullCanBeParameter)
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

  test("size of unsupported types throws errors") {
    val emptyParam: Parameter = EmptyValue
    assertThrows[IllegalArgumentException] {
      emptyParam.size
    }

    val emptyRawParam: Parameter = RawValue(0, 0, isBinary = false, Array())
    assertThrows[IllegalArgumentException] {
      emptyRawParam.size
    }
  }

  test("write of unsupported types throws errors") {
    val bufWriter = MysqlBuf.writer(Array())

    val emptyParam: Parameter = EmptyValue
    assertThrows[IllegalArgumentException] {
      emptyParam.writeTo(bufWriter)
    }

    val nullParam: Parameter = NullValue
    assertThrows[IllegalArgumentException] {
      nullParam.writeTo(bufWriter)
    }

    val emptyRawParam: Parameter = RawValue(0, 0, isBinary = false, Array())
    assertThrows[IllegalArgumentException] {
      emptyRawParam.writeTo(bufWriter)
    }
  }
}
