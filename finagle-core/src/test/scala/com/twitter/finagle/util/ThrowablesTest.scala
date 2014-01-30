package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThrowablesTest extends FunSuite {
  test("Throwables.mkString: flatten non-nested exception") {
    val t = new Throwable
    assert(Throwables.mkString(t) === Seq(t.getClass.getName))
  }

  test("Throwables.mkString: flatten nested causes into a Seq") {
    val a = new IllegalArgumentException
    val b = new RuntimeException(a)
    val c = new Throwable(b)

    assert(Throwables.mkString(c) === Seq(c.getClass.getName, b.getClass.getName, a.getClass.getName))
  }

  test("Throwables.mkString: return empty Seq on null exception") {
    assert(Throwables.mkString(null) === Seq.empty)
  }
}
