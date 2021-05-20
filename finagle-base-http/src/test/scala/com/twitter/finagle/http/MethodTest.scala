package com.twitter.finagle.http

import org.scalatest.funsuite.AnyFunSuite

class MethodTest extends AnyFunSuite {

  val knownMethods = Set(
    Method.Get,
    Method.Post,
    Method.Put,
    Method.Head,
    Method.Patch,
    Method.Delete,
    Method.Trace,
    Method.Connect,
    Method.Options
  )

  test("basic equality") {
    assert(Method.Get == Method("get"))
    assert(Method("foo") == Method("foo"))
  }

  test("apply(..) of known method names should return the cached instances") {
    assert(Method("GET") eq Method.Get)
    assert(Method("get") eq Method.Get)
  }

  test("apply(..) is case insensitive for known method names") {
    knownMethods.foreach { m => assert(Method(m.name.toUpperCase) eq Method(m.name.toLowerCase)) }
  }

  test("case sensitive for unknown method names") {
    val lowerName = "lower"
    assert(Method(lowerName) ne Method(lowerName.toUpperCase))
    assert(Method(lowerName) != Method(lowerName.toUpperCase))
  }

  test("pattern matching works") {
    Method.Get match {
      case Method.Get => assert(true)
      case _ => fail()
    }

    Method("get") match {
      case Method.Get => assert(true)
      case _ => fail()
    }
  }

  test("name") {
    Method.Get.name == "GET"
    Method("name").name == "name"
  }

  test("hashCode") {
    assert(Method.Get.hashCode == Method("GET").hashCode)
    assert(Method.Get.hashCode == Method("get").hashCode)

    assert(Method("foo").hashCode == Method("foo").hashCode)
  }
}
