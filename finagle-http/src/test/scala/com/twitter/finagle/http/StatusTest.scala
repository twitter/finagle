package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StatusTest extends FunSuite {
  test("sanity check") {
    assert(Status(199).reason == "Informational")
    assert(Status(299).reason == "Successful")
    assert(Status(399).reason == "Redirection")
    // 499 is a common error code so we use 498
    assert(Status(498).reason == "Client Error")
    assert(Status(599).reason == "Server Error")
    assert(Status(601).reason == "Unknown Status")
  }
}
