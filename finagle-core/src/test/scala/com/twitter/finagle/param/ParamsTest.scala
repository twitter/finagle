package com.twitter.finagle.param

import org.scalatest.funsuite.AnyFunSuite

class ParamsTest extends AnyFunSuite {
  test("Tags") {
    val tags = Tags("goodnight", "moon")
    assert(tags.matchAny("goodnight"))
    assert(!tags.matchAny("goodbye"))
    assert(tags.matchAll("goodnight"))
    assert(tags.matchAll("goodnight", "moon"))
    assert(!tags.matchAll("goodnight", "moon", "stars"))
  }
}
