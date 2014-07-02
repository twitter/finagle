package com.twitter.finagle.http

import com.twitter.conversions.time._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CookieTest extends FunSuite {

  test("mutate underlying") {
    val cookie = new Cookie("name", "value")
    cookie.comment    = "hello"
    cookie.commentUrl = "hello.com"
    cookie.domain     = ".twitter.com"
    cookie.maxAge     = 100.seconds
    cookie.path       = "/1/statuses/show"
    cookie.ports      = Seq(1, 2, 3)
    cookie.value      = "value2"
    cookie.version    = 1
    cookie.httpOnly   = true
    cookie.isDiscard  = false
    cookie.isSecure   = true

    cookie.name       === "name"
    cookie.comment    === "hello"
    cookie.commentUrl === "hello.com"
    cookie.domain     === ".twitter.com"
    cookie.maxAge     === 100.seconds
    cookie.path       === "/1/statuses/show"
    cookie.ports      === Set(1, 2, 3)
    cookie.value      === "value2"
    cookie.version    === 1
    cookie.httpOnly   === true
    cookie.isDiscard  === false
    cookie.isSecure   === true
  }
}
