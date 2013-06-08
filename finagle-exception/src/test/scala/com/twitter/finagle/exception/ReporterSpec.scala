package com.twitter.finagle.exception

import com.twitter.util._
import com.twitter.finagle.util.LoadedReporterFactory
import java.net.{InetAddress, InetSocketAddress}
import com.twitter.finagle.exception.thrift.{ResultCode, LogEntry, scribe}
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.anyObject
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DefaultReporterTest extends FunSuite with MockitoSugar {
  val logger = mock[scribe.FinagledClient]
  when(logger.log(anyObject())) thenReturn(Future.value(ResultCode.Ok))

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
  val logger = mock[scribe.FinagledClient]
  when(logger.log(anyObject())) thenReturn(Future.value(ResultCode.Ok))

  val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])

  val reporter = Reporter(logger, "service16").withClient()

  val tse = new TestServiceException("service16", "my cool message",
    clientAddress = Some(InetAddress.getLocalHost.getHostAddress))

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
  val logger = mock[scribe.FinagledClient]
  when(logger.log(anyObject())) thenReturn(Future.value(ResultCode.Ok))

  val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])

  val socket = new InetSocketAddress("localhost", 5871)
  val reporter = Reporter(logger, "service16")
    .withSource(socket)
    .withClient()

  val tse = new TestServiceException("service16", "my cool message",
    clientAddress = Some(InetAddress.getLocalHost.getHostAddress), sourceAddress = Some(socket.getAddress.getHostName))

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

  test("logs an exception through the loaded reporter") {
    val logger = mock[scribe.FinagledClient]
    when(logger.log(anyObject())) thenReturn(Future.value(ResultCode.Ok))
    val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])
    val tse = new TestServiceException("service", "my cool message")

    val reporter = LoadedReporterFactory("service", None).asInstanceOf[Reporter]
    reporter.copy(client = logger).handle(tse.throwable)
    verify(logger).log(captor.capture())
  }

  test("logs a client exception through the loaded reporter") {
    val logger = mock[scribe.FinagledClient]
    when(logger.log(anyObject())) thenReturn(Future.value(ResultCode.Ok))
    val captor = ArgumentCaptor.forClass(classOf[Seq[LogEntry]])
    val socket = new InetSocketAddress("localhost", 5871)
    val tse = new TestServiceException("service", "my cool message",
      clientAddress = Some(socket.getAddress.getHostName))

    val reporter = LoadedReporterFactory("service", Some(socket)).asInstanceOf[Reporter]
    reporter.copy(client = logger).handle(tse.throwable)
    verify(logger).log(captor.capture())
  }
}
