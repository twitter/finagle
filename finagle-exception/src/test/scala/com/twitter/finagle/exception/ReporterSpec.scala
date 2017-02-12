package com.twitter.finagle.exception

import com.twitter.finagle.exception.thriftscala.{LogEntry, ResultCode, Scribe}
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.anyObject
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DefaultReporterTest extends FunSuite with MockitoSugar {
  val logger = mock[Scribe.FutureIface]
  when(logger.log(anyObject())).thenReturn(Future.value(ResultCode.Ok))

  val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])

  val reporter = Reporter(logger, "service16")

  val tse = new TestServiceException("service16", "my cool message")

  test("log entries to a client once upon receive") {
    reporter.handle(tse.throwable)
    verify(logger).log(captor.capture())
  }

  test("log a json entry with the proper format") {
    val es = captor.getValue
    assert(es.size == 1)

    tse.verifyCompressedJSON(es(0).message)
  }
}

@RunWith(classOf[JUnitRunner])
class ClientReporterTest extends FunSuite with MockitoSugar {
  val logger = mock[Scribe.FutureIface]
  when(logger.log(anyObject())).thenReturn(Future.value(ResultCode.Ok))

  val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])

  val reporter = Reporter(logger, "service16").withClient()

  val tse = new TestServiceException("service16", "my cool message",
    clientAddress = Some(InetAddress.getLoopbackAddress.getHostAddress))

  test("log entries to a client once upon receive") {
    reporter.handle(tse.throwable)
    verify(logger).log(captor.capture())
  }

  test("log a json entry with the proper format") {
    val es = captor.getValue
    assert(es.size == 1)

    tse.verifyCompressedJSON(es(0).message)
  }
}

@RunWith(classOf[JUnitRunner])
class SourceClientReporterTest extends FunSuite with MockitoSugar {
  val logger = mock[Scribe.FutureIface]
  when(logger.log(anyObject())).thenReturn(Future.value(ResultCode.Ok))

  val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])

  val socket = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
  val reporter = Reporter(logger, "service16")
    .withSource(socket)
    .withClient()

  val tse = new TestServiceException("service16", "my cool message",
    clientAddress = Some(InetAddress.getLoopbackAddress.getHostAddress), sourceAddress = Some(socket.getAddress.getHostName))

  test("log entries to a client once upon receive") {
    reporter.handle(tse.throwable)
    verify(logger).log(captor.capture())
  }

  test("log a json entry with the proper format") {
    val es = captor.getValue
    assert(es.size == 1)

    tse.verifyCompressedJSON(es(0).message)
  }
}

@RunWith(classOf[JUnitRunner])
class ExceptionReporterTest extends FunSuite with MockitoSugar {

  test("logs an exception") {
    val logger = mock[Scribe.FutureIface]
    when(logger.log(anyObject())).thenReturn(Future.value(ResultCode.Ok))
    val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])
    val tse = new TestServiceException("service", "my cool message")

    val reporter = new ExceptionReporter().apply("service", None)
    reporter.copy(client = logger).handle(tse.throwable)
    verify(logger).log(captor.capture())
  }

  test("logs a client exception") {
    val logger = mock[Scribe.FutureIface]
    when(logger.log(anyObject())).thenReturn(Future.value(ResultCode.Ok))
    val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])
    val socket = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val tse = new TestServiceException("service", "my cool message",
      clientAddress = Some(socket.getAddress.getHostName))

    val reporter = new ExceptionReporter().apply("service", Some(socket))
    reporter.copy(client = logger).handle(tse.throwable)
    verify(logger).log(captor.capture())
  }

  test("appends the client address to the exception when provided") {
    val reporter = new ExceptionReporter
    val addr = new InetSocketAddress("8.8.8.8", 342)
    val factoryWithClient = reporter("qux", Some(addr))
    val factoryWithout = reporter("qux", None)

    assert(factoryWithClient != factoryWithout)
    assert(factoryWithClient == factoryWithout.withClient(addr.getAddress))
  }
}
