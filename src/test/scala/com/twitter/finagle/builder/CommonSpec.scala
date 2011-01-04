package com.twitter.finagle.builder

import java.util.concurrent.TimeUnit

import org.specs.Specification

import com.twitter.conversions.time._

object CommonSpec extends Specification {
  "Timeout" should {
    "be invertible" in {
      Timeout(10, TimeUnit.MILLISECONDS).duration must be_==(10.milliseconds)
    }
  }
}
