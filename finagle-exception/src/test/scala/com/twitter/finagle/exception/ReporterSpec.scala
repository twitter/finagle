package com.twitter.finagle.exception

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.ArgumentCaptor
import com.twitter.util.Future
import org.apache.scribe.{ResultCode, LogEntry, scribe}
import org.mockito.Matchers._
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}

object ReporterSpec extends Specification with Mockito {
  "A default chickadee reporter" should {
    setSequential()

    val logger = mock[scribe.ServiceToClient]
    logger.Log(anyObject()) returns Future(ResultCode.OK)

    val captor = ArgumentCaptor.forClass(classOf[java.util.List[LogEntry]])

    val reporter = Reporter(logger, "service16")

    val tse = new TestServiceException("service16", "my cool message")

    "log entries to a client once upon receive" in {
      reporter.handle(tse.throwable)
      there was one(logger).Log(captor.capture())
    }

    "log a json entry with the proper format" in {
      val es = captor.getValue
      es.size mustEqual 1

      tse.verifyCompressedJSON(es.get(0).getMessage)
    }
  }

  "A client-logging chickadee reporter" should {
    setSequential()

    val logger = mock[scribe.ServiceToClient]
    logger.Log(anyObject()) returns Future(ResultCode.OK)

    val captor = ArgumentCaptor.forClass(classOf[java.util.List[LogEntry]])

    val reporter = Reporter(logger, "service16").withClient()

    val tse = new TestServiceException("service16", "my cool message", clientAddress = Some(InetAddress.getLocalHost.getHostAddress))

    "log entries to a client once upon receive" in {
      reporter.handle(tse.throwable)
      there was one(logger).Log(captor.capture())
    }

    "log a json entry with the proper format" in {
      val es = captor.getValue
      es.size mustEqual 1

      tse.verifyCompressedJSON(es.get(0).getMessage)
    }
  }

  "A chickadee reporter that logs source and client" should {
    setSequential()

    val logger = mock[scribe.ServiceToClient]
    logger.Log(anyObject()) returns Future(ResultCode.OK)

    val captor = ArgumentCaptor.forClass(classOf[java.util.List[LogEntry]])

    val socket = new InetSocketAddress("localhost", 5871)
    val reporter = Reporter(logger, "service16")
      .withSource(socket)
      .withClient()

    val tse = new TestServiceException("service16", "my cool message", clientAddress = Some(InetAddress.getLocalHost.getHostAddress), sourceAddress = Some(socket.getAddress.getHostAddress + ":" + socket.getPort))

    "log entries to a client once upon receive" in {
      reporter.handle(tse.throwable)
      there was one(logger).Log(captor.capture())
    }

    "log a json entry with the proper format" in {
      val es = captor.getValue
      es.size mustEqual 1

      tse.verifyCompressedJSON(es.get(0).getMessage)
    }
  }
}
