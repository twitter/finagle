package com.twitter.finagle.liveness

import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class FailureDetectorTest extends AnyFunSuite {
  def ping = () => Future.Done
  val statsReceiver = NullStatsReceiver

  test("default settings with flag override") {
    sessionFailureDetector.let("threshold") {
      val FailureDetector.Param(failDetectorConfig) = FailureDetector.Param.param.default
      assert(
        FailureDetector(failDetectorConfig, ping, statsReceiver)
          .isInstanceOf[ThresholdFailureDetector]
      )
    }
  }

  test("flag settings with flag set to none") {
    sessionFailureDetector.let("none") {
      assert(
        NullFailureDetector == FailureDetector(
          FailureDetector.GlobalFlagConfig,
          ping,
          statsReceiver
        )
      )
    }
  }

  test("flag settings with invalid string") {
    sessionFailureDetector.let("tacos") {
      assert(
        NullFailureDetector == FailureDetector(
          FailureDetector.GlobalFlagConfig,
          ping,
          statsReceiver
        )
      )
    }
  }

  test("flag settings with valid string") {
    sessionFailureDetector.let("threshold") {
      assert(
        FailureDetector(FailureDetector.GlobalFlagConfig, ping, statsReceiver)
          .isInstanceOf[ThresholdFailureDetector]
      )
    }
  }

  test("request null gets null") {
    assert(NullFailureDetector == FailureDetector(FailureDetector.NullConfig, ping, statsReceiver))
  }

  test("explicit threshold used") {
    assert(
      FailureDetector(FailureDetector.ThresholdConfig(), ping, statsReceiver)
        .isInstanceOf[ThresholdFailureDetector]
    )
  }
}
