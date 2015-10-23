package com.twitter.finagle.util

import com.twitter.finagle.Failure
import com.twitter.logging.{BareFormatter, StringHandler, Level, Logger}
import org.junit.runner.RunWith
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DefaultMonitorTest extends FunSuite
  with Matchers
{
  private val handler = new StringHandler(BareFormatter, Some(Level.DEBUG))
  private val logger = Logger.get("DefaultMonitorTest")
  logger.addHandler(handler)
  logger.setLevel(Level.DEBUG)

  test("Failures with low log levels are handled") {
    handler.clear()
    val monitor = new DefaultMonitor(logger)

    val f = Failure("debug handled").withLogLevel(Level.DEBUG)
    assert(monitor.handle(f))

    handler.get should include("Exception propagated to DefaultMonitor")
  }

  test("other Failures and Exceptions are not handled") {
    handler.clear()
    val monitor = new DefaultMonitor(logger)

    val f = Failure("info not handled").withLogLevel(Level.INFO)
    assert(monitor.handle(f)) // still handled, but by the RootMonitor
    assert(handler.get == "")

    assert(monitor.handle(new RuntimeException())) // still handled, but by the RootMonitor
    assert(handler.get == "")
  }

}
