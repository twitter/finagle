package com.twitter.finagle

import org.scalatest.FunSuite
import org.scalatest.junit.AssertionsForJUnit

class PathTest extends FunSuite with AssertionsForJUnit {
  test("Path.show") {
    assert(NameTreeParsers.parsePath("/foo/bar").show == "/foo/bar")
  }
}
