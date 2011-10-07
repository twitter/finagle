package com.twitter.finagle.http

import org.specs.Specification

object ProxyCredentialsSpec extends Specification {
  "BasicProxyCredentials" should {
    "add Proxy-Authorization header" in {
      val creds = ProxyCredentials("foo", "bar")
      creds.basicAuthorization must_== "Basic Zm9vOmJhcg=="
    }
  }
}
