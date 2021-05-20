package com.twitter.finagle.util

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.RemoteInfo.Upstream
import com.twitter.finagle.{Failure, TimeoutException}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Duration, TimeoutException => UtilTimeoutException}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

// This class exists because mockito is not good at
// mocking Scala methods which use by-name parameters.
// The `DefaultMonitor` class uses `Logger`'s 'logLazy'
// method, which does use by-name parameters. Therefore
// we create a fake class with a concrete method that
// we can override to handle the argument capture
// ourselves directly.
class FakeLogger extends Logger("fake", null) {

  var capturedLevel: Level = _
  var capturedThrown: Throwable = _
  var capturedMessage: String = _

  override def logLazy(level: Level, thrown: Throwable, message: => AnyRef): Unit = {
    capturedLevel = level
    capturedThrown = thrown
    capturedMessage = message.toString
  }

}

class DefaultMonitorTest extends AnyFunSuite with BeforeAndAfterEach {

  private var monitor: DefaultMonitor = _

  private var log: FakeLogger = _

  override def beforeEach(): Unit = {
    log = new FakeLogger()
    monitor = new DefaultMonitor(log, "n/a", "n/a")
  }

  private def verifyPublished(
    expectedLevel: Level,
    expectedThrown: Throwable,
    expectedMessageFragment: String = "Exception propagated to the default monitor"
  ): Unit = {
    assert(expectedLevel == log.capturedLevel)
    assert(expectedThrown == log.capturedThrown)
    assert(log.capturedMessage.contains(expectedMessageFragment))
  }

  private[this] class MyTimeoutException(
    protected val timeout: Duration,
    protected val explanation: String)
      extends TimeoutException

  test("Failures with low log levels are handled") {
    val f = Failure("debug handled").withLogLevel(Level.DEBUG)
    assert(monitor.handle(f))
    verifyPublished(f.logLevel, f)
  }

  test("c.t.util.TimeoutExceptions are handled") {
    val f = new UtilTimeoutException("7 minute abs")
    assert(monitor.handle(f))
    verifyPublished(Level.TRACE, f)
  }

  test("wrapped c.t.util.TimeoutExceptions are handled") {
    val f = new RuntimeException(new UtilTimeoutException("6 minute abs"))
    assert(monitor.handle(f))
    verifyPublished(Level.TRACE, f)
  }

  test("HasLogLevel wrapped c.t.util.TimeoutExceptions are handled") {
    val f = Failure
      .wrap(new UtilTimeoutException("6 minute abs"))
      .withLogLevel(Level.DEBUG)
    assert(monitor.handle(f))
    verifyPublished(Level.TRACE, f)
  }

  test("HasLogLevel is respected") {
    val f = Failure("debug handled").withLogLevel(Level.FATAL)
    assert(monitor.handle(f))
    verifyPublished(f.logLevel, f)
  }

  test("c.t.finagle.TimeoutExceptions are handled") {
    val f = new MyTimeoutException(30.seconds, "5 minute abs")
    assert(monitor.handle(f))
    verifyPublished(f.logLevel, f)
  }

  test("peer information is logged") {
    val f = new Exception("error")
    monitor = new DefaultMonitor(log, "foo", "bar")

    Contexts.local.let(Upstream.AddressCtx, InetSocketAddressUtil.unconnected) {
      assert(monitor.handle(f))
    }

    val remoteInfo = "(upstream address: unconnected, downstream address: bar, label: foo)"
    verifyPublished(Level.WARNING, f, remoteInfo)
  }
}
