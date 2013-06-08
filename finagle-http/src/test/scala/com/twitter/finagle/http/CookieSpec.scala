package com.twitter.finagle.http

import com.twitter.conversions.time._
import org.specs.SpecificationWithJUnit

class CookieSpec extends SpecificationWithJUnit {
  "Cookie" should {
    "mutate underlying" in {
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

      cookie.name       must_== "name"
      cookie.comment    must_== "hello"
      cookie.commentUrl must_== "hello.com"
      cookie.domain     must_== ".twitter.com"
      cookie.maxAge     must_== 100.seconds
      cookie.path       must_== "/1/statuses/show"
      cookie.ports      must_== Set(1, 2, 3)
      cookie.value      must_== "value2"
      cookie.version    must_== 1
      cookie.httpOnly   must_== true
      cookie.isDiscard  must_== false
      cookie.isSecure   must_== true
    }
  }
}
