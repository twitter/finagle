package com.twitter.finagle.http

import com.twitter.conversions.time._
import org.jboss.netty.handler.codec.http.DefaultCookie
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class CookieTest extends FunSuite {

  test("mutate underlying") {
    val cookie = new Cookie("name", "value")
    cookie.comment = "hello"
    cookie.commentUrl = "hello.com"
    cookie.domain = ".twitter.com"
    cookie.maxAge = 100.seconds
    cookie.path = "/1/statuses/show"
    cookie.ports = Seq(1, 2, 3)
    cookie.value = "value2"
    cookie.version = 1
    cookie.httpOnly = true
    cookie.isDiscard = false
    cookie.isSecure = true

    assert(cookie.name == "name")
    assert(cookie.comment == "hello")
    assert(cookie.commentUrl == "hello.com")
    assert(cookie.domain == ".twitter.com")
    assert(cookie.maxAge == 100.seconds)
    assert(cookie.path == "/1/statuses/show")
    assert(cookie.ports == Set(1, 2, 3))
    assert(cookie.value == "value2")
    assert(cookie.version == 1)
    assert(cookie.httpOnly == true)
    assert(cookie.isDiscard == false)
    assert(cookie.isSecure == true)
  }

  test("constructor sets correct params") {
    val cookie = new Cookie(
      name = "name",
      value = "value",
      domain = Some("domain"),
      path = Some("path"),
      maxAge = Some(99.seconds),
      secure = true,
      httpOnly = false
    )

    assert(cookie.name == "name")
    assert(cookie.value == "value")
    assert(cookie.domain == "domain")
    assert(cookie.path == "path")
    assert(cookie.maxAge == 99.seconds)
    assert(cookie.secure == true)
    assert(cookie.httpOnly == false)
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

  test("default are the same as DefaultCookie default") {
    val cookie = new Cookie("name", "value")
    val nettyCookie = new DefaultCookie("name", "value")

    assert(cookie.name == nettyCookie.getName)
    assert(cookie.value == nettyCookie.getValue)
    assert(cookie.domain == nettyCookie.getDomain)
    assert(cookie.path == nettyCookie.getPath)
    assert(cookie.comment == nettyCookie.getComment)
    assert(cookie.commentUrl == nettyCookie.getCommentUrl)
    assert(cookie.discard == nettyCookie.isDiscard)
    assert(cookie.ports == nettyCookie.getPorts.asScala.toSet)
    assert(cookie.maxAge == nettyCookie.getMaxAge.seconds)
    assert(cookie.version == nettyCookie.getVersion)
    assert(cookie.secure == nettyCookie.isSecure)
    assert(cookie.httpOnly == nettyCookie.isHttpOnly)
  }

  test("Throws exception if name is empty") {
    intercept[IllegalArgumentException] {
      new Cookie("    ", "value")
    }
  }

  test("Throws exception if name starts with $") {
    intercept[IllegalArgumentException] {
      new Cookie("$dolladollabillz", "value")
    }
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

  test("ports are validated") {
    intercept[IllegalArgumentException] {
      val cookie = new Cookie("name", "value")
      cookie.ports = Seq(-1, 0, 5) // cannot be negative
    }

    val cookie = new Cookie("name", "value")
    intercept[IllegalArgumentException] {
      cookie.ports = Seq(-1, 0, 5) // cannot be negative
    }
  }

  private[this] val IllegalFieldChars = Set('\n', '\u000b', '\f', '\r', ';')

  test("path trimmed and validated") {
    val cookie = new Cookie("name", "value", path = Some("   /path"))
    assert(cookie.path == "/path")

    IllegalFieldChars.foreach { c =>
      intercept[IllegalArgumentException] {
        cookie.path = s"hello${c}goodbye"
      }
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
        cookie.domain = s"hello${c}goodbye"
      }
      intercept[IllegalArgumentException] {
        new Cookie("name", "value", domain = Some(s"hello${c}goodbye"))
      }
    }
  }

  test("comment trimmed and validated") {
    val cookie = new Cookie("name", "value")
    cookie.comment = "   comment   "
    assert(cookie.comment == "comment")

    IllegalFieldChars.foreach { c =>
      intercept[IllegalArgumentException] {
        cookie.path = s"hello${c}goodbye"
      }
      intercept[IllegalArgumentException] {
        val cookie = new Cookie("name", "value")
        cookie.comment = s"hello${c}goodbye"
      }
    }
  }

  test("commentUrl trimmed and validated") {
    val cookie = new Cookie("name", "value")
    cookie.commentUrl = "   commentUrl   "
    assert(cookie.commentUrl == "commentUrl")

    IllegalFieldChars.foreach { c =>
      intercept[IllegalArgumentException] {
        cookie.path = s"hello${c}goodbye"
      }
      intercept[IllegalArgumentException] {
        val cookie = new Cookie("name", "value")
        cookie.commentUrl = s"hello${c}goodbye"
      }
    }
  }

  test(
    "methods that copy existing params and create a new Cookie with an additional configured param "
  ) {
    val cookie = new Cookie("name", "value")
      .domain(Some("domain"))
      .maxAge(Some(99.seconds))
      .httpOnly(true)
      .secure(true)

    assert(cookie.name == "name")
    assert(cookie.value == "value")
    assert(cookie.domain == "domain")
    assert(cookie.maxAge == 99.seconds)
    assert(cookie.httpOnly == true)
    assert(cookie.secure == true)
  }
}
