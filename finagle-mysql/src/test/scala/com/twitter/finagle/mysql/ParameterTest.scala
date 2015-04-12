package com.twitter.finagle.mysql

import com.twitter.finagle.exp.mysql.Parameter
import com.twitter.finagle.exp.mysql.Parameter.NullParameter
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParameterTest extends FunSuite {
  test("null, when directly given a type Parameter, stays unchanged") {
    val x: Parameter = null
    assert(x == null)
  }

  test("null with a static type that is coercible to Parameter, " +
    "when given a type Parameter, coerces to NullParameter") {
    val y: String = null
    val x: Parameter = y
    assert(x == NullParameter)
  }

  test("A value with a static type that is coercible to Parameter, " +
    "when given a type Parameter, coerces while preserving original value internally") {
    val x: Parameter = "Howdy"
    assert(x.value == "Howdy")
  }
}
