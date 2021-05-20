package com.twitter.finagle.http

import com.twitter.finagle.http.Status._
import org.scalatest.funsuite.AnyFunSuite

class StatusTest extends AnyFunSuite {
  test("sanity check") {
    assert(Status(199).reason == "Informational")
    assert(Status(299).reason == "Successful")
    assert(Status(399).reason == "Redirection")
    // 499 is a common error code so we use 498
    assert(Status(498).reason == "Client Error")
    assert(Status(599).reason == "Server Error")
    assert(Status(601).reason == "Unknown Status")
  }

  test("fromCode (known)") {
    assert(Status.Ok == Status.fromCode(200))
  }

  test("fromCode (unknown)") {
    assert(Status(1234) == Status.fromCode(1234))
  }

  test("matches unknown status") {
    Status(99) match {
      case Unknown(_) =>
      case status => fail(s"$status should be UnknownStatus")
    }
  }

  test("matches informational status") {
    100.until(200).foreach { code =>
      Status(code) match {
        case Informational(_) =>
        case status => fail(s"$status should be Informational")
      }
    }
  }

  test("matches successful status") {
    200.until(300).foreach { code =>
      Status(code) match {
        case Successful(_) =>
        case status => fail(s"$status should be Successful")
      }
    }
  }

  test("matches redirection status") {
    300.until(400).foreach { code =>
      Status(code) match {
        case Redirection(_) =>
        case status => fail(s"$status should be Redirection")
      }
    }
  }

  test("match client error status") {
    400.until(500).foreach { code =>
      Status(code) match {
        case ClientError(_) =>
        case status => fail(s"$status should be ClientError")
      }
    }
  }

  test("match server error status") {
    500.until(600).foreach { code =>
      Status(code) match {
        case ServerError(_) =>
        case status => fail(s"$status should be ServerError")
      }
    }
  }

  test("match unknown status") {
    600.until(700).foreach { code =>
      Status(601) match {
        case Unknown(_) =>
        case status => fail(s"$status should be UnknownStatus")
      }
    }
  }
}
