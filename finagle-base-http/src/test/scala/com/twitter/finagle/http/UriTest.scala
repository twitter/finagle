package com.twitter.finagle.http

import org.scalatest.funsuite.AnyFunSuite

class UriTest extends AnyFunSuite {
  test("Can construct a URI from a request") {
    val req = Request("/whatever")
    req.host = "twitter.com"
    val uri = Uri.fromRequest(req)
    assert(uri.toString == "twitter.com/whatever")
  }

  test("Can construct a URI from a request that has a query string") {
    val req = Request("/whatever?foo=bar")
    req.host = "twitter.com"
    val uri = Uri.fromRequest(req)
    assert(uri.toString == "twitter.com/whatever?foo=bar")
  }

  test("Can construct a URI from a request that doesn't have a host header") {
    val req = Request("/whatever?foo=bar")
    val uri = Uri.fromRequest(req)
    assert(uri.toString == "/whatever?foo=bar")
  }

  test("Can extract a URI from a query string") {
    val uri = new Uri("twitter.com", "/whatever", "foo=bar")
    assert(uri.params.toMap == Map("foo" -> "bar"))
  }

  test("Provides path when no query parameters") {
    val req = Request("/abcdef")
    val uri = Uri.fromRequest(req)
    assert(uri.path == "/abcdef")
  }

  test("Provides path without query parameters") {
    val req = Request("/abc/def?ghi=jkl&mno=pqr")
    val uri = Uri.fromRequest(req)
    assert(uri.path == "/abc/def")
  }
}
