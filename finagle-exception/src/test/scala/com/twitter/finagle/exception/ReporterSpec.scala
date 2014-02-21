package com.twitter.finagle.exception

import com.twitter.finagle.exception.thrift.scribe
import com.twitter.finagle.exception.thrift.{ResultCode, LogEntry}
import com.twitter.finagle.util.LoadedReporterFactory
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import java.util.{List => JList}
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.anyObject
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class DefaultReporterTest extends FunSuite with MockitoSugar {
  val logger = mock[scribe.ServiceToClient]
  when(logger.Log(anyObject())) thenReturn(Future.value(ResultCode.OK))

  val captor = ArgumentCaptor.forClass(classOf[JList[LogEntry]])

  val reporter = Reporter(logger, "service16")

  val tse = new TestServiceException("service16", "my cool message")

  test("log entries to a client once upon receive") {
    reporter.handle(tse.throwable)
    verify(logger).Log(captor.capture())
  }

  test("log a json entry with the proper format") {
    val es = captor.getValue.asScala
    assert(es.size == 1)

    tse.verifyCompressedJSON(es(0).message)
  }
}

@RunWith(classOf[JUnitRunner])
class ClientReporterTest extends FunSuite with MockitoSugar {
  val logger = mock[scribe.ServiceToClient]
  when(logger.Log(anyObject())) thenReturn(Future.value(ResultCode.OK))

  val captor = ArgumentCaptor.forClass(classOf[JList[LogEntry]])

  val reporter = Reporter(logger, "service16").withClient()

  val tse = new TestServiceException("service16", "my cool message",
    clientAddress = Some(InetAddress.getLocalHost.getHostAddress))

  test("log entries to a client once upon receive") {
    reporter.handle(tse.throwable)
    verify(logger).Log(captor.capture())
  }

  test("log a json entry with the proper format") {
    val es = captor.getValue.asScala
    assert(es.size == 1)

    tse.verifyCompressedJSON(es(0).message)
  }
}

@RunWith(classOf[JUnitRunner])
class SourceClientReporterTest extends FunSuite with MockitoSugar {
  val logger = mock[scribe.ServiceToClient]
  when(logger.Log(anyObject())) thenReturn(Future.value(ResultCode.OK))

  val captor = ArgumentCaptor.forClass(classOf[JList[LogEntry]])

  val socket = new InetSocketAddress("localhost", 5871)
  val reporter = Reporter(logger, "service16")
    .withSource(socket)
    .withClient()

  val tse = new TestServiceException("service16", "my cool message",
    clientAddress = Some(InetAddress.getLocalHost.getHostAddress), sourceAddress = Some(socket.getAddress.getHostName))

  test("log entries to a client once upon receive") {
    reporter.handle(tse.throwable)
    verify(logger).Log(captor.capture())
  }

  test("log a json entry with the proper format") {
    val es = captor.getValue.asScala
    assert(es.size == 1)

    tse.verifyCompressedJSON(es(0).message)
  }
}

@RunWith(classOf[JUnitRunner])
class ExceptionReporterTest extends FunSuite with MockitoSugar {

  test("logs an exception through the loaded reporter") {
    val logger = mock[scribe.ServiceToClient]
    when(logger.Log(anyObject())) thenReturn(Future.value(ResultCode.OK))
    val captor = ArgumentCaptor.forClass(classOf[JList[LogEntry]])
    val tse = new TestServiceException("service", "my cool message")

    val reporter = LoadedReporterFactory("service", None).asInstanceOf[Reporter]
    reporter.copy(client = logger).handle(tse.throwable)
    verify(logger).Log(captor.capture())
  }

  test("logs a client exception through the loaded reporter") {
    val logger = mock[scribe.ServiceToClient]
    when(logger.Log(anyObject())) thenReturn(Future.value(ResultCode.OK))
    val captor = ArgumentCaptor.forClass(classOf[JList[LogEntry]])
    val socket = new InetSocketAddress("localhost", 5871)
    val tse = new TestServiceException("service", "my cool message",
      clientAddress = Some(socket.getAddress.getHostName))

    val reporter = LoadedReporterFactory("service", Some(socket)).asInstanceOf[Reporter]
    reporter.copy(client = logger).handle(tse.throwable)
    verify(logger).Log(captor.capture())
  }
}
