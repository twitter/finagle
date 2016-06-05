package com.twitter.finagle

import com.twitter.finagle.context.RemoteInfo
import com.twitter.finagle.thrift.ClientId
import com.twitter.finagle.tracing.{Trace, SpanId, TraceId}
import com.twitter.util.Duration
import java.net.{InetSocketAddress, SocketAddress}

import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ExceptionsTest extends FunSuite with MockitoSugar {
  trait ExceptionsHelper {
    val address = mock[SocketAddress]
    when(address.toString).thenReturn("foo")
    val underlying = mock[Throwable]
    when(underlying.getMessage).thenReturn("bar")
  }

  test("ChannelException should not generate message when all parameters are null") {
    new ExceptionsHelper {
      val ex = new ChannelException(null, null)
      assert(ex.getMessage == null)
    }
  }

  test("ChannelException should generate message with address info when address is provided") {
    new ExceptionsHelper {
      val ex = new ChannelException(null, address)
      assert(ex.getMessage.contains("foo"))
    }
  }

  test("ChannelException should generate message with underlying exception info when exception is provided") {
    new ExceptionsHelper {
      val ex = new ChannelException(underlying, null)
      assert(!(ex.getMessage == null))
    }
  }

  test("ChannelException should generate message with correct info when all parameters are provided") {
    new ExceptionsHelper {
      val ex = new ChannelException(underlying, address)
      assert(ex.getMessage.contains("foo"))
      assert(ex.getMessage.contains("bar"))
    }
  }

  test("ChannelException should generate message with service name when it's available") {
    new ExceptionsHelper {
      val ex = new ChannelException(null, null)
      ex.serviceName = "foo"
      assert(ex.getMessage.contains("foo"))
    }
  }

  test("ChannelException should provide access to remote address") {
    new ExceptionsHelper {
      val ex = new ChannelException(underlying, address)
      assert(ex.remoteAddress == address)
    }
  }

  test("WriteException should apply and unapply") {
    val rootCause = new RuntimeException("howdy")
    val writeEx = WriteException(rootCause)
    assert(writeEx.getCause == rootCause)

    writeEx match {
      case WriteException(cause) => assert(cause == rootCause)
    }
  }

  test("WriteException should no cause") {
    val writeEx = WriteException(null)
    writeEx match {
      case WriteException(cause) => assert(cause == null)
    }
  }

  test("ServiceTimeoutException should have a good explanation when filled in") {
    val exc = new ServiceTimeoutException(Duration.Top)
    exc.serviceName = "finagle"
    assert(exc.getMessage.contains(exc.serviceName))
  }

  test("SourcedException extractor understands SourceException") {
    val exc = new ServiceTimeoutException(Duration.Top)

    assert(SourcedException.unapply(exc) == None)

    exc.serviceName = "finagle"

    assert(SourcedException.unapply(exc) == Some("finagle"))
  }

  test("SourcedException extractor understands Failure") {
    val exc = Failure(new Exception(""))

    assert(SourcedException.unapply(exc) == None)

    val finagleExc = exc.withSource(Failure.Source.Service, "finagle")

    assert(SourcedException.unapply(finagleExc) == Some("finagle"))
  }

  test("HasRemoteInfo exception contains remote info in message") {
    val exc = new HasRemoteInfo {
      override def exceptionMessage = "foo"
    }
    val traceId = TraceId(None, None, SpanId(1L), None)
    val downstreamAddr = new InetSocketAddress("1.2.3.4", 100)
    val downstreamId = "downstream"
    val upstreamAddr = new InetSocketAddress("2.3.4.5", 100)
    val upstreamId = "upstream"

    exc.setRemoteInfo(RemoteInfo.Available(Some(upstreamAddr), Some(ClientId(upstreamId)), Some(downstreamAddr), Some(ClientId(downstreamId)), traceId))
    assert(exc.getMessage() == "foo. Remote Info: Upstream Address: /2.3.4.5:100, Upstream Client Id: upstream, " +
      "Downstream Address: /1.2.3.4:100, Downstream Client Id: downstream, " +
      s"Trace Id: $traceId")
  }

  test("NoBrokersAvailableException includes dtabs in error message") {
    val ex = new NoBrokersAvailableException(
      "/s/cool/story",
      Dtab.base,
      Dtab.read("/foo=>/$/com.twitter.butt")
    )

    assert(ex.getMessage ==
      "No hosts are available for /s/cool/story, " +
      s"Dtab.base=[${Dtab.base.show}], " +
      "Dtab.local=[/foo=>/$/com.twitter.butt]. " +
      "Remote Info: Not Available"
    )
  }
}
