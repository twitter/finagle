package com.twitter.finagle.netty4.http.util

import org.scalatest.funsuite.AnyFunSuite

class UriUtilsTest extends AnyFunSuite {

  test("accepts acceptable ascii characters") {
    assert(UriUtils.isValidUri("/abc/xyz/123.jpg"))
  }

  test("rejects URI with non-encoded special characters") {
    assert(UriUtils.isValidUri("/abc xyz 123.jpg") == false)
    assert(UriUtils.isValidUri("/DSC02175拷貝.jpg") == false)
  }

  test("accepts URI with query string") {
    assert(UriUtils.isValidUri("/abc/xyz?query1=param1"))
  }

  test("accepts URI with URL encoded characters") {
    assert(UriUtils.isValidUri("/abc%20xyz%3D.jpg"))
    assert(UriUtils.isValidUri("/abc+xyz%3D.jpg"))
    assert(UriUtils.isValidUri("/abc/123/abc%2FKf23%2F4hQrHM%3D"))
    assert(UriUtils.isValidUri("/%2F2F%3D"))
  }

  test("accepts URI with query string with non-encoded characters") {
    assert(UriUtils.isValidUri("/abc/xyz?query1=param 1234&query 2=891"))
  }

  test("rejects URI with illegal escape characters") {
    assert(UriUtils.isValidUri("/1%%") == false)
    assert(UriUtils.isValidUri("/1%") == false)
    assert(UriUtils.isValidUri("/1%F%F") == false)
    assert(UriUtils.isValidUri("/%%%") == false)
    assert(UriUtils.isValidUri("/%2%F") == false)
  }

}
