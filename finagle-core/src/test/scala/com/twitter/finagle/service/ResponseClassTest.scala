package com.twitter.finagle.service

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResponseClassTest extends FunSuite {

  test("validates fractionalSuccess") {
    intercept[IllegalArgumentException] { ResponseClass.Successful(0.0) }
    intercept[IllegalArgumentException] { ResponseClass.Successful(1.1) }
  }

}
