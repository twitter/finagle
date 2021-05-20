package com.twitter.finagle.http

import org.scalatest.funsuite.AnyFunSuite

class ProxyCredentialsTest extends AnyFunSuite {
  test("add Proxy-Authorization header") {
    val creds = ProxyCredentials("foo", "bar")
    assert(creds.basicAuthorization == "Basic Zm9vOmJhcg==")
  }

  test("construct via Map") {
    val creds = ProxyCredentials(
      Map(
        "http_proxy_user" -> "foo",
        "http_proxy_pass" -> "bar"
      )
    ).get
    assert(creds.basicAuthorization == "Basic Zm9vOmJhcg==")
  }
}
