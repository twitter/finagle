package com.twitter.finagle.util

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class PathTest extends FunSuite {

  test("Path.join") {
    assert(Path.join("/a/b", "c/d") === "/a/b/c/d")
    assert(Path.join("/a/b", "/c/d") === "/a/b/c/d")
    assert(Path.join("//a/b", "/c/d") === "/a/b/c/d")
    assert(Path.join("//a/b", "") === "/a/b")
    assert(Path.join("", "/a/b/") === "/a/b")
  }
  
  test("Path.split") {
    assert(Path.split("/a//b/c/d/e/") === Seq("a", "b", "c", "d", "e"))
  }
}
