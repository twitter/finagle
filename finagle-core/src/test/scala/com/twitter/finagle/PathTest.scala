package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class PathTest extends FunSuite with AssertionsForJUnit {
  test("Path.show") {
    assert(NameTreeParsers.parsePath("/foo/bar").show == "/foo/bar")
  }
}
