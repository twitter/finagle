package com.twitter.finagle.util

import com.twitter.conversions.time._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.RemoteInfo.Upstream
import com.twitter.finagle.{Failure, TimeoutException}
import com.twitter.logging.{BareFormatter, Level, Logger, StringHandler}
import com.twitter.util.{Duration, TimeoutException => UtilTimeoutException}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DefaultMonitorTest extends FunSuite
  with Matchers
{
  private[this] class MyTimeoutException(
    protected val timeout: Duration,
    protected val explanation: String
  ) extends TimeoutException

  private val handler = new StringHandler(BareFormatter, Some(Level.TRACE))
  private val logger = Logger.get("DefaultMonitorTest")
  logger.addHandler(handler)
  logger.setLevel(Level.TRACE)

  test("Failures with low log levels are handled") {
    handler.clear()
    val monitor = new DefaultMonitor(logger, "n/a", "n/a")

    val f = Failure("debug handled").withLogLevel(Level.DEBUG)
    assert(monitor.handle(f))

    handler.get should include("Exception propagated to the default monitor")
  }

  test("c.t.util.TimeoutExceptions are handled") {
    handler.clear()
    val monitor = new DefaultMonitor(logger, "n/a", "n/a")

    assert(monitor.handle(new UtilTimeoutException("7 minute abs")))

    handler.get should include("Exception propagated to the default monitor")
  }

  test("c.t.finagle.TimeoutExceptions are handled") {
    handler.clear()
    val monitor = new DefaultMonitor(logger, "n/a", "n/a")

    assert(monitor.handle(new MyTimeoutException(30.seconds, "5 minute abs")))

    handler.get should include("Exception propagated to the default monitor")
  }

  test("peer information is logged") {
    handler.clear()
    val monitor = new DefaultMonitor(logger, "foo", "bar")

    Contexts.local.let(Upstream.AddressCtx, InetSocketAddressUtil.unconnected) {
      assert(monitor.handle(new Exception("error")))
    }

    val remoteInfo = s"(upstream address: unconnected, downstream address: bar, label: foo)"

    handler.get should include(remoteInfo)
  }
}
