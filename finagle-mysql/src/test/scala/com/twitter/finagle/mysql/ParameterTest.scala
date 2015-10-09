package com.twitter.finagle.mysql

import com.twitter.finagle.exp.mysql.Parameter
import com.twitter.finagle.exp.mysql.Parameter.NullParameter
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParameterTest extends FunSuite {
  test("Parameter coercion") {
    locally {
      val x: Parameter = null
      assert(x == null)
    }

    locally {
      val y: String = null
      val x: Parameter = y
      assert(x == NullParameter)
    }

    locally {
      val x: Parameter = "Howdy"
      assert(x.value == "Howdy")
    }
  }

  test("Parameter.unsafeWrap(null)") {
    assert(Parameter.unsafeWrap(null) == NullParameter)
  }
}
