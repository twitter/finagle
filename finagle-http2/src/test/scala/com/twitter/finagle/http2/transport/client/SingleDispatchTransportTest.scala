package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Awaitable, Duration, Future}
import io.netty.buffer.{ByteBufAllocator, EmptyByteBuf}
import io.netty.handler.codec.http.{DefaultHttpContent, DefaultLastHttpContent, HttpContent}
import org.mockito.Mockito._
import org.scalatest.OneInstancePerTest
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class SingleDispatchTransportTest extends AnyFunSuite with MockitoSugar with OneInstancePerTest {

  private[this] def await[T](t: Awaitable[T]): T =
    Await.result(t, Duration.fromSeconds(5))

  private[this] val underlying = mock[Transport[Any, Any]]
  when(underlying.status).thenReturn(Status.Open)
  private[this] val wrapped = new SingleDispatchTransport(underlying)

  private[this] def httpContent(last: Boolean): HttpContent = {
    val data = new EmptyByteBuf(ByteBufAllocator.DEFAULT)
    if (last) new DefaultLastHttpContent(data)
    else new DefaultHttpContent(data)
  }

  test("status is the underlying status before reading LastHttpContent") {
    assert(wrapped.status == Status.Open)
    verify(underlying, times(1)).status
    when(underlying.read()).thenReturn(Future.value(httpContent(false)))

    await(wrapped.read())

    assert(wrapped.status == Status.Open)
    verify(underlying, times(1)).read()
    verify(underlying, times(2)).status

  }

  test("status is closed after reading LastHttpContent") {
    assert(wrapped.status == Status.Open)
    verify(underlying, times(1)).status
    when(underlying.read()).thenReturn(Future.value(httpContent(true)))

    await(wrapped.read())

    assert(wrapped.status == Status.Closed)
    verify(underlying, times(1)).read()
    verify(underlying, times(1)).status
  }
}
