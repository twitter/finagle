package com.twitter.finagle.util

import com.twitter.conversions.time._
import com.twitter.util.Duration
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class parsersTest extends FunSuite {
  import parsers._

  test("double") {
    "123.123" match {
      case double(123.123) =>
      case _ => fail()
    }
    
    "abc" match {
      case double(_) => fail()
      case _ =>
    }
  }
  
  test("int") {
    "123" match {
      case int(123) =>
      case _ => fail()
    }
    
    "abc" match {
      case int(_) => fail()
      case _ =>
    }
  }
  
  test("duration") {
    "10.seconds" match {
      case duration(d) if d == 10.seconds =>
      case _ => fail()
    }
    
    "10" match {
      case duration(_) => fail()
      case _ =>
    }
  }
  
  test("list") {
    "a:b:c" match {
      case list("a", "b", "c") =>
      case _ => fail()
    }
    
    "" match {
      case list() =>
      case _ => fail()
    }
    
    "10.seconds:abc:123.32:999" match {
      case list(duration(d), "abc", double(123.32), int(999)) if d == 10.seconds =>
      case _ => fail()
    }
    
    "foo:bar:baz" match {
      case list(elems@_*) =>
        assert(elems == Seq("foo", "bar", "baz"))
      case _ => fail()
    }
  }
}
