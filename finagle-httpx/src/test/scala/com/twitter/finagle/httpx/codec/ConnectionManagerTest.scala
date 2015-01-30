package com.twitter.finagle.httpx.codec

import com.twitter.finagle.httpx.{
  HttpTransport, Ask, Version, Method, Response, Fields, Status
}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Promise, Return, Future, Time}
import java.net.InetSocketAddress
import java.nio.charset.Charset
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http.{HttpRequest=>HttpAsk, HttpResponse}
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

  def makeAsk(version: Version, headers: (String, String)*) = {
    val ask = Ask(version, Method.Get, "/")
    headers foreach { case (k, v) =>
      ask.headers.set(k, v)
    }
    ask
  }

  def makeResponse(version: Version, headers: (String, String)*) = {
    val response = Response(version, Status.Ok)
    headers foreach { case (k, v) =>
      response.headers.set(k, v)
    }
    response
  }

  def perform(request: Ask, response: Response, shouldMarkDead: Boolean) {
    val trans = mock[Transport[Any, Any]]
    when(trans.close(any[Time])).thenReturn(Future.Done)
    when(trans.close).thenReturn(Future.Done)

    val disp = new HttpClientDispatcher(new HttpTransport(trans))

    val wp = new Promise[Unit]
    when(trans.write(any[HttpAsk])).thenReturn(wp)

    val f = disp(request)
    assert(f.isDefined === false)

    verify(trans, times(1)).write(any[HttpAsk])
    verify(trans, never()).read()

    val rp = new Promise[HttpResponse]
    when(trans.read()).thenReturn(rp)

    wp.setDone()

    verify(trans, times(1)).read()

    assert(f.isDefined === false)
    rp.setValue(response.httpResponse)

    f.poll match {
      case Some(Return(r)) =>
        assert(r.version === response.version)
        assert(r.status === response.status)

      case _ =>
        fail()
    }

    if (shouldMarkDead)
      verify(trans, times(1)).close
  }

  test("not terminate regular http/1.1 connections") {
    perform(
      makeAsk(Version.Http11),
      makeResponse(Version.Http11, Fields.ContentLength -> "1"),
      false)
  }

  // Note: by way of the codec, this reply is already taken care of.
  test("terminate http/1.1 connections without content length") {
    perform(
      makeAsk(Version.Http11),
      makeResponse(Version.Http11),
      true
    )
  }

  test("terminate http/1.1 connections with Connection: close") {
    perform(
      makeAsk(Version.Http11, "Connection" -> "close"),
      makeResponse(Version.Http11),
      true
    )
  }
}
