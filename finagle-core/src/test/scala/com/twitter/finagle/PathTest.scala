package com.twitter.finagle

import org.scalatestplus.junit.AssertionsForJUnit
import org.scalatest.funsuite.AnyFunSuite

class PathTest extends AnyFunSuite with AssertionsForJUnit {
  test("Path.show") {
    assert(NameTreeParsers.parsePath("/foo/bar").show == "/foo/bar")
  }
}
