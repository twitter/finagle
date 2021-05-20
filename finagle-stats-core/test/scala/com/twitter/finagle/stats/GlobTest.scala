package com.twitter.finagle.stats

import org.scalatest.funsuite.AnyFunSuite

class GlobTest extends AnyFunSuite {

  test("exact") {
    assert(Glob("foobar")("foobar"))
    assert(!Glob("foobar")("foobarr"))
    assert(!Glob("fooba")("foobar"))
  }

  test("single wildcard") {
    assert(Glob("*")(""))
    assert(Glob("*")("foo"))
    assert(Glob("foo*")("foobar"))
    assert(Glob("foo*")("foo"))
    assert(!Glob("foo*")("bar"))
    assert(Glob("*foo")("foo"))
    assert(Glob("*foo")("barfoo"))
    assert(!Glob("*foo")("foobar"))
  }

  test("two wildcards") {
    assert(Glob("**")(""))
    assert(Glob("*foo*")("foo"))
    assert(Glob("*foo*")("barfoobaz"))
    assert(Glob("*foo*")("foobar"))
    assert(Glob("*foo*")("barfoo"))
    assert(!Glob("*foo*")("bar"))
  }

  test("two segments") {
    assert(Glob("foo*bar")("foobar"))
    assert(Glob("foo*bar")("foobazbar"))
    assert(!Glob("foo*bar")("foo"))
    assert(!Glob("foo*bar")("bar"))
  }

  test("comma (or)") {
    assert(Glob("foo,bar")("foo"))
    assert(Glob("foo,bar")("bar"))
    assert(!Glob("foo,bar")("baz"))
    assert(Glob("foo,*")("baz"))
    assert(Glob("foo,*")("foo"))
    assert(Glob("*,foo")("foo"))
    assert(Glob("*,foo")("bar"))
    assert(Glob("*,foo")("foo"))
    assert(Glob("foo*foo,*bar")("foofoo"))
    assert(Glob("foo*foo,*bar")("foodfoo"))
    assert(Glob("foo*foo,*bar")("foobar"))
    assert(Glob("foo*foo,*bar")("bar"))
    assert(Glob("foo*foo,*bar")("barbar"))
    assert(!Glob("foo*foo,*bar")("barbaz"))
  }
}
