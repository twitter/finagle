package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.cookie.SameSite
import org.scalatest.funsuite.AnyFunSuite

class CookieTest extends AnyFunSuite {

  test("constructor sets correct params") {
    val cookie = new Cookie(
      name = "name",
      value = "value",
      domain = Some("domain"),
      path = Some("path"),
      maxAge = Some(99.seconds),
      secure = true,
      httpOnly = false,
      sameSite = SameSite.Strict
    )

    assert(cookie.name == "name")
    assert(cookie.value == "value")
    assert(cookie.domain == "domain")
    assert(cookie.path == "path")
    assert(cookie.maxAge == 99.seconds)
    assert(cookie.secure)
    assert(!cookie.httpOnly)

    /* Experimental */
    assert(cookie.sameSite == SameSite.Strict)
  }

  test("equals: not equal if object is different") {
    val foo = "hello"
    val cookie = new Cookie("name", "value")
    assert(!cookie.equals(foo))
  }

  test("equals: not equal if names are not equal") {
    val c1 = new Cookie(name = "name1", "value")
    val c2 = new Cookie(name = "name2", "value")
    assert(!c1.equals(c2))
  }

  test("equals: Not equal if paths are not equal") {
    // first path is null
    val c1 = new Cookie("name", "value", path = None)
    val c2 = new Cookie("name", "value", path = Some("path"))
    assert(!c1.equals(c2))

    // second path is null
    val c3 = new Cookie("name", "value", path = Some("path"))
    val c4 = new Cookie("name", "value", path = None)
    assert(!c3.equals(c4))

    // both paths exist
    val c5 = new Cookie("name", "value", path = Some("path1"))
    val c6 = new Cookie("name", "value", path = Some("path2"))
    assert(!c5.equals(c6))
  }

  test("equals: Not equal if domains are not equal") {
    // first domain is null
    val c1 = new Cookie("name", "value", path = Some("path"), domain = None)
    val c2 = new Cookie("name", "value", path = Some("path"), domain = Some("domain"))
    assert(!c1.equals(c2))

    // second domain is null
    val c3 = new Cookie("name", "value", path = Some("path"), domain = Some("domain"))
    val c4 = new Cookie("name", "value", path = Some("path"), domain = None)
    assert(!c3.equals(c4))

    // both domains exist
    val c5 = new Cookie("name", "value", path = Some("path"), domain = Some("domain1"))
    val c6 = new Cookie("name", "value", path = Some("path"), domain = Some("domain2"))
    assert(!c5.equals(c6))
  }

  test("equals: equal if names/paths/domains are equal") {
    // path and domain both null
    val c1 = new Cookie("name", "value", path = None, domain = None)
    val c2 = new Cookie("name", "value", path = None, domain = None)
    assert(c1.equals(c2))

    // domain null
    val c3 = new Cookie("name", "value", path = Some("path"), domain = None)
    val c4 = new Cookie("name", "value", path = Some("path"), domain = None)
    assert(c3.equals(c4))

    // path and domain non-null
    val c5 = new Cookie("name", "value", path = Some("path"), domain = Some("domain"))
    val c6 = new Cookie("name", "value", path = Some("path"), domain = Some("domain"))
    assert(c5.equals(c6))
  }

  test("Throws exception if name is empty") {
    intercept[IllegalArgumentException] {
      new Cookie("    ", "value")
    }
  }

  test("Does not throw exception if name starts with $") {
    new Cookie("$dolladollabillz", "value")
  }

  test("Throws exception if name contains illegal char") {
    Set('\t', '\n', '\u000b', '\f', '\r', ' ', ',', ';', '=').foreach { c =>
      intercept[IllegalArgumentException] {
        new Cookie(s"hello${c}goodbye", "value")
      }
    }
  }

  test("name is trimmed") {
    val cookie = new Cookie("    name     ", "value")
    assert(cookie.name == "name")
  }

  private[this] val IllegalFieldChars = Set('\n', '\u000b', '\f', '\r', ';')

  test("path trimmed and validated") {
    val cookie = new Cookie("name", "value", path = Some("   /path"))
    assert(cookie.path == "/path")

    IllegalFieldChars.foreach { c =>
      intercept[IllegalArgumentException] {
        new Cookie("name", "value", path = Some(s"hello${c}goodbye"))
      }
    }
  }

  test("domain trimmed and validated") {
    val cookie = new Cookie("name", "value", domain = Some("   domain"))
    assert(cookie.domain == "domain")

    IllegalFieldChars.foreach { c =>
      intercept[IllegalArgumentException] {
        new Cookie("name", "value", domain = Some(s"hello${c}goodbye"))
      }
    }
  }

  test(
    "methods that copy existing params and create a new Cookie with an additional configured param "
  ) {
    val cookie = new Cookie("name", "value")
      .domain(Some("domain"))
      .value("value2")
      .maxAge(Some(99.seconds))
      .httpOnly(true)
      .secure(true)
      .sameSite(SameSite.Lax)

    assert(cookie.name == "name")
    assert(cookie.value == "value2")
    assert(cookie.domain == "domain")
    assert(cookie.maxAge == 99.seconds)
    assert(cookie.httpOnly)
    assert(cookie.secure)

    /* Experimental */
    assert(cookie.sameSite == SameSite.Lax)
  }

  test("maxAge is default if set to None") {
    val cookie = new Cookie("name", "value").maxAge(None)
    assert(cookie.maxAge == Cookie.DefaultMaxAge)
  }
}
