package com.twitter.finagle.memcached.unit.protocol

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.memcached.protocol.ExpiryValidation
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class ExpiryValidationTest extends AnyFunSuite {

  private[this] val command = "TestCommand"

  test("Time.epoch is a valid expiry") {
    assert(ExpiryValidation.checkExpiry(command, Time.epoch))
  }

  test("Expiry in the future is a valid expiry") {
    assert(ExpiryValidation.checkExpiry(command, 1.hour.fromNow))
  }

  test("Expiry in the past is not a valid expiry") {
    assert(!ExpiryValidation.checkExpiry(command, Time.now - 1.hour))
  }
}
