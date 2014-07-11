package com.twitter.finagle.http.codec

import com.twitter.finagle.http.HttpTransport
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Promise, Return, Future, Time}
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ConnectionManagerTest extends FunSuite with MockitoSugar {
  // > further tests
  //   - malformed requests/responses
  //   - methods other than GET
  //   - 100/continue

  val me = mock[MessageEvent]
  val c = mock[Channel]
  val cFuture = new DefaultChannelFuture(c, false)
  when(me.getChannel).thenReturn(c)

  def makeRequest(version: HttpVersion, headers: (String, String)*) = {
    val request = new DefaultHttpRequest(version, HttpMethod.GET, "/")
    headers foreach { case (k, v) =>
      request.headers.set(k, v)
    }

    request
  }

  def makeResponse(version: HttpVersion, headers: (String, String)*) = {
    val response = new DefaultHttpResponse(version, HttpResponseStatus.OK)
    headers foreach { case (k, v) =>
      response.headers.set(k, v)
    }

    response
  }

  def perform(request: HttpRequest, response: HttpResponse, shouldMarkDead: Boolean) {
    val trans = mock[Transport[Any, Any]]
    when(trans.close(any[Time])).thenReturn(Future.Done)
    when(trans.close).thenReturn(Future.Done)

    val disp = new HttpClientDispatcher[HttpRequest](new HttpTransport(trans))

    val wp = new Promise[Unit]
    when(trans.write(any[HttpRequest])).thenReturn(wp)

    val f = disp(request)
    assert(f.isDefined === false)

    verify(trans, times(1)).write(request)
    verify(trans, never()).read()

    val rp = new Promise[HttpResponse]
    when(trans.read()).thenReturn(rp)

    wp.setDone()

    verify(trans, times(1)).read()

    assert(f.isDefined === false)
    rp.setValue(response)

    f.poll match {
      case Some(Return(r: HttpResponse)) =>
        assert(r.version === response.getProtocolVersion)
        assert(r.status === response.getStatus)

      case _ =>
        fail()
    }

    if (shouldMarkDead)
      verify(trans, times(1)).close
  }

  test("not terminate regular http/1.1 connections") {
    perform(
      makeRequest(HttpVersion.HTTP_1_1),
      makeResponse(HttpVersion.HTTP_1_1, HttpHeaders.Names.CONTENT_LENGTH -> "1"),
      false)
  }

  // Note: by way of the codec, this reply is already taken care of.
  test("terminate http/1.1 connections without content length") {
    perform(
      makeRequest(HttpVersion.HTTP_1_1),
      makeResponse(HttpVersion.HTTP_1_1),
      true
    )
  }

  test("terminate http/1.1 connections with Connection: close") {
    perform(
      makeRequest(HttpVersion.HTTP_1_1, "Connection" -> "close"),
      makeResponse(HttpVersion.HTTP_1_1),
      true
    )
  }
}
