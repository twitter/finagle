package com.twitter.finagle.http2

import org.mockito.Mockito._
import org.mockito.Matchers._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Status
import com.twitter.finagle.http.Multi
import com.twitter.finagle.http.Request
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.StreamTransport
import com.twitter.finagle.http2.transport.client.Http2Transport
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Time
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class Http2TransportTest extends AnyFunSuite with OneInstancePerTest with MockitoSugar {

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, 30.seconds)

  private[this] val readP = Promise[Multi[Response]]()
  private[this] val writeP = Promise[Unit]
  private[this] val closeP = Promise[Unit]

  private[this] val streamTransport = {
    val t = mock[StreamTransport[Request, Response]]

    when(t.status).thenReturn(Status.Open)

    when(t.read()).thenReturn(readP)
    when(t.write(any())).thenReturn(writeP)

    when(t.close()).thenReturn(closeP)
    when(t.close(any(classOf[Time]))).thenReturn(closeP)
    when(t.close(any(classOf[Duration]))).thenReturn(closeP)

    t
  }

  private[this] val http2Transport = new Http2Transport(streamTransport)

  test("propagates the underlying transport status") {
    assert(http2Transport.status == Status.Open)

    when(streamTransport.status).thenReturn(Status.Closed)
    assert(http2Transport.status == Status.Closed)
  }

  test("Allows a single simple dispatch") {
    readP.setValue(Multi(Response(), Future.Done))
    writeP.setDone()

    assert(http2Transport.status == Status.Open)
    await(http2Transport.write(Request()))
    assert(http2Transport.status == Status.Open)
    await(http2Transport.read())
    assert(http2Transport.status == Status.Closed)
  }

  test("dispatch where write finishes before read") {
    val readFinished = Promise[Unit]()
    readP.setValue(Multi(Response(), readFinished))
    writeP.setDone()

    assert(http2Transport.status == Status.Open)
    await(http2Transport.write(Request()))
    assert(http2Transport.status == Status.Open)
    await(http2Transport.read())
    assert(http2Transport.status == Status.Open)
    readFinished.setDone()

    assert(http2Transport.status == Status.Closed)
    verify(streamTransport, times(1)).close()
  }

  test("dispatch where read finishes before write") {
    readP.setValue(Multi(Response(), Future.Done))

    assert(http2Transport.status == Status.Open)
    val writeF = http2Transport.write(Request())

    assert(http2Transport.status == Status.Open)
    assert(!writeF.isDefined)

    val Multi(_, readFinished) = await(http2Transport.read())
    assert(readFinished.isDefined)
    assert(http2Transport.status == Status.Open)

    writeP.setDone()
    assert(http2Transport.status == Status.Closed)
    verify(streamTransport, times(1)).close()
  }
}
