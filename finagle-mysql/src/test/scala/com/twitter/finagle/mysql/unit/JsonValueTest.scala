package com.twitter.finagle.mysql.unit

import com.twitter.finagle.mysql.{JsonValue, MysqlCharset, RawValue, Type}
import org.scalatest.funsuite.AnyFunSuite

class JsonValueTest extends AnyFunSuite {
  private val jsonStr = """{"foo": "quick"}"""

  test("read json value bytes") {
    val rawJsonValue = RawValue(Type.Json, MysqlCharset.Binary, false, jsonStr.getBytes)
    assert(Some(jsonStr) == JsonValue.fromValue(rawJsonValue).map(new String(_)))
  }

  test("None if RawValue is not of type json") {
    val rawStringValue = RawValue(Type.String, MysqlCharset.Binary, false, jsonStr.getBytes)
    assert(None == JsonValue.fromValue(rawStringValue))
  }
}
