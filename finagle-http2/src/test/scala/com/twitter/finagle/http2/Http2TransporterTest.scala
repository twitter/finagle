package com.twitter.finagle.http2

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Stack, Status}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.transport.{ClientSession, Http2UpgradingTransport}
import com.twitter.finagle.http2.transport.Http2UpgradingTransport._
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.util.{Await, Duration, Future, MockTimer, Promise}
import io.netty.handler.codec.http._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class Http2TransporterTest extends FunSuite with MockitoSugar {
  def await[T](f: Future[T], wait: Duration = 1.second) =
    Await.result(f, wait)

  test("Http2Transporter uses http11 for the second outstanding transport preupgrade") {
    val t1 = mock[Transporter[Any, Any, TransportContext]]
    when(t1.apply()).thenReturn(Future.never)

    val t2 = mock[Transporter[Any, Any, TransportContext]]
    when(t2.apply()).thenReturn(Future.never)

    val transporter = new Http2Transporter(t1, t2, false, Stack.Params.empty, new MockTimer())

    val ft1 = transporter()
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()

    assert(!ft1.isDefined)
    transporter()
    verify(t1, times(1)).apply()
    verify(t2, times(1)).apply()
  }

  test("Http2Transporter reuses the http2 transporter postupgrade") {
    val clientSession = mock[ClientSession]
    val firstTransport = mock[Transport[Any, Any]]
    when(firstTransport.read()).thenReturn(Future.never)

    val childTransport = mock[Transport[Any, Any]]
    when(clientSession.newChildTransport()).thenReturn(Future.value(childTransport))

    val read1 = new Object

    val t1 = mock[Transporter[Any, Any, TransportContext]]
    val upgradingTransport = mock[Transport[Any, Any]]
    when(upgradingTransport.onClose).thenReturn(Future.never)

    when(t1.apply()).thenReturn(Future.value(upgradingTransport))
    when(upgradingTransport.write(any())).thenReturn(Future.Done)
    when(upgradingTransport.read()).thenReturn(Future.value(UpgradeSuccessful { _ =>
      val first = mock[Transport[Any, Any]]
      when(first.read()).thenReturn(Future.value(read1))
      clientSession -> first
    }))

    val t2 = mock[Transporter[Any, Any, TransportContext]]
    val transporter = new Http2Transporter(t1, t2, false, Stack.Params.empty, new MockTimer())

    val trans = await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()

    trans.write(LastHttpContent.EMPTY_LAST_CONTENT)
    assert(await(trans.read()) == read1)
    verify(clientSession, times(0)).newChildTransport()

    await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()
    verify(clientSession, times(1)).newChildTransport()
  }

  test("Http2Transporter uses the http11 transporter post rejection") {
    val t1 = mock[Transporter[Any, Any, TransportContext]]
    val upgradingTransport = mock[Transport[Any, Any]]
    when(upgradingTransport.onClose).thenReturn(Future.never)
    when(upgradingTransport.write(any())).thenReturn(Future.Done)
    val read1 = new Object
    when(upgradingTransport.read()).thenReturn(
      Future.value(Http2UpgradingTransport.UpgradeRejected),
      Future.value(read1) // second read
    )

    when(t1.apply()).thenReturn(Future.value(upgradingTransport))

    val t2 = mock[Transporter[Any, Any, TransportContext]]
    val t2transport = mock[Transport[Any, Any]]
    when(t2.apply()).thenReturn(Future.value(t2transport))

    val transporter = new Http2Transporter(t1, t2, false, Stack.Params.empty, new MockTimer())

    val trans = await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()

    trans.write(LastHttpContent.EMPTY_LAST_CONTENT)
    assert(await(trans.read()) == read1)
    verify(upgradingTransport, times(1)).write(LastHttpContent.EMPTY_LAST_CONTENT)
    verify(upgradingTransport, times(2)).read()

    assert(await(transporter()) == t2transport)
    verify(t1, times(1)).apply()
    verify(t2, times(1)).apply()
  }

  test("Http2Transporter marks outstanding transports dead after a successful upgrade") {
    val clientSession = mock[ClientSession]
    when(clientSession.status).thenReturn(Status.Open)

    val t1 = mock[Transporter[Any, Any, TransportContext]]
    val upgradingTransport = mock[Transport[Any, Any]]
    when(upgradingTransport.onClose).thenReturn(Future.never)
    when(upgradingTransport.write(any())).thenReturn(Future.Done)
    val first = mock[Transport[Any, Any]]
    val read1 = new Object
    when(upgradingTransport.read()).thenReturn(Future.value(UpgradeSuccessful { _ =>
      when(first.read()).thenReturn(Future.value(read1))
      clientSession -> first
    }))

    when(t1.apply()).thenReturn(Future.value(upgradingTransport))

    val t2 = mock[Transporter[Any, Any, TransportContext]]
    val t2transport = mock[Transport[Any, Any]]
    when(t2.apply()).thenReturn(Future.value(t2transport))
    when(t2transport.status).thenReturn(Status.Open)

    val transporter = new Http2Transporter(t1, t2, false, Stack.Params.empty, new MockTimer())

    val trans = await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()

    val http11Trans = await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(1)).apply()
    assert(http11Trans.status == Status.Open)

    trans.write(LastHttpContent.EMPTY_LAST_CONTENT)
    assert(await(trans.read()) == read1)
    verify(upgradingTransport, times(1)).write(LastHttpContent.EMPTY_LAST_CONTENT)
    verify(upgradingTransport, times(1)).read()
    verify(first, times(1)).read()

    assert(http11Trans.status == Status.Closed)
  }

  test("Http2Transporter keeps outstanding transports alive after a failed upgrade") {
    val t1 = mock[Transporter[Any, Any, TransportContext]]
    val upgradingTransport = mock[Transport[Any, Any]]
    when(upgradingTransport.onClose).thenReturn(Future.never)
    when(upgradingTransport.write(any())).thenReturn(Future.Done)
    val read1 = new Object
    when(upgradingTransport.read()).thenReturn(
      Future.value(Http2UpgradingTransport.UpgradeRejected),
      Future.value(read1) // second read
    )

    when(t1.apply()).thenReturn(Future.value(upgradingTransport))

    val t2 = mock[Transporter[Any, Any, TransportContext]]
    val t2transport = mock[Transport[Any, Any]]
    when(t2.apply()).thenReturn(Future.value(t2transport))
    when(t2transport.status).thenReturn(Status.Open)

    val transporter = new Http2Transporter(t1, t2, false, Stack.Params.empty, new MockTimer())

    val trans = await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()

    val http11Trans = await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(1)).apply()
    assert(http11Trans.status == Status.Open)

    trans.write(LastHttpContent.EMPTY_LAST_CONTENT)
    assert(await(trans.read()) == read1)
    verify(upgradingTransport, times(1)).write(LastHttpContent.EMPTY_LAST_CONTENT)
    verify(upgradingTransport, times(2)).read()

    assert(http11Trans.status == Status.Open)
  }

  test("Http2Transporter doesn't mark outstanding transports dead after a failed connect attempt") {
    val p = Promise[Transport[Any, Any]]()
    val t1 = mock[Transporter[Any, Any, TransportContext]]
    when(t1.apply()).thenReturn(p)

    val t2 = mock[Transporter[Any, Any, TransportContext]]
    val t2transport = mock[Transport[Any, Any]]
    when(t2.apply()).thenReturn(Future.value(t2transport))
    when(t2transport.status).thenReturn(Status.Open)

    val transporter = new Http2Transporter(t1, t2, false, Stack.Params.empty, new MockTimer())

    val trans = transporter()
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()

    val http11Trans = await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(1)).apply()
    assert(http11Trans.status == Status.Open)

    assert(!trans.isDefined)
    val e = new Exception("boom!")
    p.setException(e)
    val actual = intercept[Exception] {
      await(trans)
    }
    assert(actual == e)
    assert(http11Trans.status == Status.Open)
  }

  test("Http2Transporter can try to establish a connection again if a connection failed") {
    val e = new Exception("boom!")

    val upgradingTransport = mock[Transport[Any, Any]]
    when(upgradingTransport.onClose).thenReturn(Future.never)

    val t1 = mock[Transporter[Any, Any, TransportContext]]
    when(t1.apply()).thenReturn(
      Future.exception(e),
      Future.value(upgradingTransport)
    )

    val t2 = mock[Transporter[Any, Any, TransportContext]]

    val transporter = new Http2Transporter(t1, t2, false, Stack.Params.empty, new MockTimer())

    val actual = intercept[Exception] {
      await(transporter())
    }
    assert(actual == e)
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()

    await(transporter())
    verify(t1, times(2)).apply()
    verify(t2, times(0)).apply()
  }

  test("Http2Transporter evicts the connection if it dies") {
    val clientSession = mock[ClientSession]
    when(clientSession.status).thenReturn(Status.Open)
    val firstTransport = mock[Transport[Any, Any]]
    when(firstTransport.read()).thenReturn(Future.never)

    val childTransport = mock[Transport[Any, Any]]
    when(clientSession.newChildTransport()).thenReturn(Future.value(childTransport))

    val readValue = new Object

    val t1 = mock[Transporter[Any, Any, TransportContext]]
    val upgradingTransport = mock[Transport[Any, Any]]
    when(upgradingTransport.onClose).thenReturn(Future.never)

    when(t1.apply()).thenReturn(Future.value(upgradingTransport))
    when(upgradingTransport.write(any())).thenReturn(Future.Done)
    when(upgradingTransport.read()).thenReturn(Future.value(UpgradeSuccessful { _ =>
      val first = mock[Transport[Any, Any]]
      when(first.read()).thenReturn(Future.value(readValue))
      clientSession -> first
    }))

    val t2 = mock[Transporter[Any, Any, TransportContext]]
    val transporter = new Http2Transporter(t1, t2, false, Stack.Params.empty, new MockTimer())

    val trans = await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()

    trans.write(LastHttpContent.EMPTY_LAST_CONTENT)
    assert(await(trans.read()) == readValue)
    verify(clientSession, times(0)).newChildTransport()

    await(transporter())
    verify(t1, times(1)).apply()
    verify(t2, times(0)).apply()
    verify(clientSession, times(1)).newChildTransport()

    // No set the status of the ClientSession to Closed so it gets evicted.
    when(clientSession.status).thenReturn(Status.Closed)

    // This should be another first transporter.
    val f2 = await(transporter())
    verify(t1, times(2)).apply()
    verify(t2, times(0)).apply()

    assert(await(f2.read()) == readValue)
  }
}
