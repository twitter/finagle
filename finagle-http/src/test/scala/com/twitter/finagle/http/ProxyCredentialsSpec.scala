package com.twitter.finagle.http

import org.specs.SpecificationWithJUnit

class ProxyCredentialsSpec extends SpecificationWithJUnit {
  "BasicProxyCredentials" should {
    "add Proxy-Authorization header" in {
      val creds = ProxyCredentials("foo", "bar")
      creds.basicAuthorization must_== "Basic Zm9vOmJhcg=="
    }
  }
}
