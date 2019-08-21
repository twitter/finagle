package com.twitter.finagle.util

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.RemoteInfo.Upstream
import com.twitter.finagle.{Failure, TimeoutException}
import com.twitter.logging.{Level, Logger}
import com.twitter.util.{Duration, TimeoutException => UtilTimeoutException}
import java.util.logging.Handler
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}

class DefaultMonitorTest extends FunSuite with Matchers with MockitoSugar with BeforeAndAfterEach {

  private var handler: Handler = _

  private var monitor: DefaultMonitor = _

  private var log: Logger = _

  override def beforeEach(): Unit = {
    handler = mock[Handler]
    log = Logger.get()
    log.setLevel(Level.TRACE)
    log.clearHandlers()
    log.addHandler(handler)
    monitor = new DefaultMonitor(log, "n/a", "n/a")
  }

  private def verifyPublished(expectedLevel: Level, expectedThrown: Throwable): Unit = {
    val capture =
      ArgumentCaptor.forClass(classOf[java.util.logging.LogRecord])
    verify(handler).publish(capture.capture())

    assert(expectedLevel == capture.getValue.getLevel)
    assert(expectedThrown == capture.getValue.getThrown)
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

    val capture =
      ArgumentCaptor.forClass(classOf[java.util.logging.LogRecord])
    verify(handler).publish(capture.capture())

    assert(Level.WARNING == capture.getValue.getLevel)
    assert(f == capture.getValue.getThrown)

    val remoteInfo = "(upstream address: unconnected, downstream address: bar, label: foo)"
    capture.getValue.getMessage should include(remoteInfo)
  }
}
