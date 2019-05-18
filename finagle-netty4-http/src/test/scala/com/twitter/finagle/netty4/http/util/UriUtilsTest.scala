package com.twitter.finagle.netty4.http.util

import org.scalatest.FunSuite

class UriUtilsTest extends FunSuite {

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
  }

  test("accepts URI with query string with non-encoded characters") {
    assert(UriUtils.isValidUri("/abc/xyz?query1=param 1234&query 2=891"))
  }

}
