package com.twitter.finagle.service

import org.scalatest.funsuite.AnyFunSuite

class ResponseClassTest extends AnyFunSuite {

  test("validates fractionalSuccess") {
    intercept[IllegalArgumentException] { ResponseClass.Successful(0.0) }
    intercept[IllegalArgumentException] { ResponseClass.Successful(1.1) }
  }

}
