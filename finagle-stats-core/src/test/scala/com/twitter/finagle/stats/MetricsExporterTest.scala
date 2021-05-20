package com.twitter.finagle.stats

import com.twitter.conversions.DurationOps._
import com.twitter.logging.{Logger, StringHandler}
import com.twitter.util.{Await, Awaitable, Duration, Time}
import org.scalatest.funsuite.AnyFunSuite

class MetricsExporterTest extends AnyFunSuite {

  def await[T](a: Awaitable[T], timeout: Duration): T = Await.result(a, timeout)

  test("close() logs metrics data when logOnShutdown is enabled") {
    logOnShutdown.let(true) {
      val handler = StringHandler()()
      val logger = Logger.get("test_enabled")
      logger.addHandler(handler)
      val exporter = new MetricsExporter(logger)
      await(exporter.close(Time.now + 500.milliseconds), 1.second)
      assert(handler.get.nonEmpty)
    }
  }

  test("close() does not log metric data when logOnShutdown is disabled") {
    logOnShutdown.let(false) {
      val handler = StringHandler()()
      val logger = Logger.get("test_disabled")
      logger.addHandler(handler)
      val exporter = new MetricsExporter(logger)
      await(exporter.close(Time.now + 500.milliseconds), 1.second)
      assert(handler.get.isEmpty)
    }
  }

}
