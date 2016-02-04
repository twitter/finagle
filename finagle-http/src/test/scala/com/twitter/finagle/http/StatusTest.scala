package com.twitter.finagle.http

import com.twitter.finagle.http.Status._
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

  test("Pattern match usability check") {
    Status(99) match {
      case UnknownStatus(_) =>
      case status => fail(s"$status should be UnknownStatus")
    }
    Status(100) match {
      case Informational(_) =>
      case status => fail(s"$status should be Informational")
    }
    Status(199) match {
      case Informational(_) =>
      case status => fail(s"$status should be Informational")
    }
    Status(299) match {
      case Successful(_) =>
      case status => fail(s"$status should be Successful")
    }
    Status(399) match {
      case Redirection(_) =>
      case status => fail(s"$status should be Redirection")
    }
    Status(489) match {
      case ClientError(_) =>
      case status => fail(s"$status should be ClientError")
    }
    Status(599) match {
      case ServerError(_) =>
      case status => fail(s"$status should be ServerError")
    }
    Status(601) match {
      case UnknownStatus(_) =>
      case status => fail(s"$status should be UnknownStatus")
    }
  }
}
