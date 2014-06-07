package com.twitter.finagle.http.codec

import com.twitter.finagle.http.HttpTransport
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Promise, Return, Future, Time}
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ConnectionManagerSpec extends SpecificationWithJUnit with Mockito {
  // > further tests
  //   - malformed requests/responses
  //   - methods other than GET
  //   - 100/continue

  val me = mock[MessageEvent]
  val c = mock[Channel]
  val ctx = mock[ChannelHandlerContext]
  val cFuture = new DefaultChannelFuture(c, false)
  me.getChannel returns c

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


  "HttpClientDispatcher" should {
    val trans = mock[Transport[Any, Any]]
    trans.close(any[Time]) returns Future.Done
    trans.close() returns Future.Done
    val disp = new HttpClientDispatcher[HttpRequest](new HttpTransport(trans))

    def perform(request: HttpRequest, response: HttpResponse, shouldMarkDead: Boolean) {
      val wp = new Promise[Unit]
      trans.write(any[HttpRequest]) returns wp
      val f = disp(request)
      f.isDefined must beFalse
      there was one(trans).write(request)
      there was no(trans).read()
      val rp = new Promise[HttpResponse]
      trans.read() returns rp
      wp.setDone()
      there was one(trans).read()
      f.isDefined must beFalse
      rp.setValue(response)
      f.poll match {
        case Some(Return(r: HttpResponse)) =>
          r.version must_== response.getProtocolVersion
          r.status must_== response.getStatus
        case _ => fail()
      }

      if (shouldMarkDead)
        there was one(trans).close()
    }

    "not terminate regular http/1.1 connections" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1),
        makeResponse(HttpVersion.HTTP_1_1, HttpHeaders.Names.CONTENT_LENGTH -> "1"),
        false)
    }

    // Note: by way of the codec, this reply is already taken care of.
    "terminate http/1.1 connections without content length" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1),
        makeResponse(HttpVersion.HTTP_1_1),
        true
      )
    }

    "terminate http/1.1 connections with Connection: close" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1, "Connection" -> "close"),
        makeResponse(HttpVersion.HTTP_1_1),
        true
      )
    }
  }
}

