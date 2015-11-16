package com.twitter.finagle.http.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StringUtilTest extends FunSuite {

  test("toSomeShort") {
    assert(StringUtil.toSomeShort("0") == 0)
    assert(StringUtil.toSomeShort("blarg") == 0)
    assert(StringUtil.toSomeShort("1000000000000") == 0)
  }

  test("toSomeInt") {
    assert(StringUtil.toSomeInt("0")             == 0)
    assert(StringUtil.toSomeInt("blarg")         == 0)
    assert(StringUtil.toSomeInt("1000000000000") == 0)
  }

  test("toSomeLong") {
    assert(StringUtil.toSomeLong("0") == 0L)
    assert(StringUtil.toSomeLong("blarg") == 0L)
    assert(StringUtil.toSomeLong("1000000000000000000000") == 0L)
  }
}
