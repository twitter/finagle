package com.twitter.finagle.util

import com.twitter.conversions.DurationOps._
import org.scalatest.funsuite.AnyFunSuite

class parsersTest extends AnyFunSuite {
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

  test("boolean") {
    "fALse" match {
      case bool(b) => assert(b == false)
      case _ => fail()
    }

    "1" match {
      case bool(b) => assert(b)
      case _ => fail()
    }

    "abc" match {
      case bool(_) => fail()
      case _ =>
    }
  }

  test("long") {
    "abc" match {
      case long(_) => fail()
      case _ =>
    }

    "2L" match {
      case long(2L) =>
      case _ => fail()
    }

    "9223372036854775807" match {
      case long(l) => assert(l == Long.MaxValue)
      case _ => fail()
    }
  }

  test("longHex") {
    "abc" match {
      case longHex(result) => assert(result == 2748L && result == 0xabc)
      case _ => fail()
    }

    "0x123" match {
      case longHex(result) => assert(result == 291L && result == 0x123)
      case _ => fail()
    }

    "invalid" match {
      case longHex(_) => fail()
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
      case list(elems @ _*) =>
        assert(elems == Seq("foo", "bar", "baz"))
      case _ => fail()
    }
  }
}
