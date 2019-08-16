package com.twitter.finagle.service

import org.scalatest.FunSuite

class ResponseClassTest extends FunSuite {

  test("validates fractionalSuccess") {
    intercept[IllegalArgumentException] { ResponseClass.Successful(0.0) }
    intercept[IllegalArgumentException] { ResponseClass.Successful(1.1) }
  }

}
