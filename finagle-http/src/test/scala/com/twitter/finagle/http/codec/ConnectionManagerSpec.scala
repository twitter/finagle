package com.twitter.finagle.http.codec

import com.twitter.finagle.transport.Transport
import com.twitter.util.{Promise, Return, Future}
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
      request.setHeader(k, v)
    }

    request
  }

  def makeResponse(version: HttpVersion, headers: (String, String)*) = {
    val response = new DefaultHttpResponse(version, HttpResponseStatus.OK)
    headers foreach { case (k, v) =>
      response.setHeader(k, v)
    }

    response
  }


  "HttpClientDispatcher" should {
    val trans = mock[Transport[HttpRequest, HttpResponse]]
    trans.close(any) returns Future.Done
    trans.close() returns Future.Done
    val disp = new HttpClientDispatcher(trans)

    def perform(request: HttpRequest, response: HttpResponse, shouldMarkDead: Boolean) {
      val wp = new Promise[Unit]
      trans.write(any) returns wp
      val f = disp(request)
      f.isDefined must beFalse
      there was one(trans).write(request)
      there was no(trans).read()
      val rp = new Promise[HttpResponse]
      trans.read() returns rp
      wp.setValue(())
      there was one(trans).read()
      f.isDefined must beFalse
      rp.setValue(response)
      f.poll must beSome(Return(response))

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

  "the server HTTP connection manager" should {
    val handler = new ServerConnectionManager
    def perform(request: HttpRequest, response: HttpResponse) {
      me.getMessage returns request
      handler.messageReceived(ctx, me)
      me.getMessage returns response

      me.getFuture returns cFuture
      handler.writeRequested(ctx, me)
    }

    "terminate http/1.0 requests" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_0),
        makeResponse(
          HttpVersion.HTTP_1_0,
          HttpHeaders.Names.CONTENT_LENGTH -> "1"))

      there was no(c).close()
      cFuture.setSuccess()   // write success
      there was one(c).close()
    }

    "terminate http/1.1 with connection: close" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1, HttpHeaders.Names.CONNECTION -> "close"),
        makeResponse(HttpVersion.HTTP_1_1, HttpHeaders.Names.CONTENT_LENGTH -> "1")
      )

      there was no(c).close()
      cFuture.setSuccess()   // write success
      there was one(c).close()
    }

    "not terminate regular http/1.1 request" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1),
        makeResponse(HttpVersion.HTTP_1_1, HttpHeaders.Names.CONTENT_LENGTH -> "1")
      )

      cFuture.setSuccess()   // write success
      there was no(c).close()
    }

    "terminate http/1.1 request with missing content-length (in the response)" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1),
        makeResponse(HttpVersion.HTTP_1_1)
      )

      there was no(c).close()
      cFuture.setSuccess()   // write success
      there was one(c).close()
    }

    "normalize header values" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1, "coNNECTion" -> "cloSE"),
        makeResponse(HttpVersion.HTTP_1_1)
      )

      there was no(c).close()
      cFuture.setSuccess()   // write success
      there was one(c).close()
    }

    "normalize header values with content length in the response" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1, "coNNECTion" -> "cloSE"),
        makeResponse(HttpVersion.HTTP_1_1, HttpHeaders.Names.CONTENT_LENGTH -> "1")
      )

      there was no(c).close()
      cFuture.setSuccess()   // write success
      there was one(c).close()
    }


    "respect server-side connection termination" in {
      perform(
        makeRequest(HttpVersion.HTTP_1_1),
        makeResponse(
          HttpVersion.HTTP_1_1,
          HttpHeaders.Names.CONTENT_LENGTH -> "1",
          HttpHeaders.Names.CONNECTION -> "close"))

      there was no(c).close()
      cFuture.setSuccess()   // write success
      there was one(c).close()
    }

    "not terminate chunked HTTP/1.1 responses" in {
      val request = makeRequest(HttpVersion.HTTP_1_1)
      val response = makeResponse(HttpVersion.HTTP_1_1)
      response.setChunked(true)

      perform(request, response)
      there was no(c).close()
      cFuture.setSuccess()   // write success
      there was no(c).close()

      val chunk = new DefaultHttpChunk(
        ChannelBuffers.copiedBuffer("content", Charset.forName("UTF-8")))

      me.getMessage returns chunk
      handler.writeRequested(ctx, me)
      me.getMessage returns chunk
      handler.writeRequested(ctx, me)

      // The final chunk.
      me.getMessage returns ChannelBuffers.EMPTY_BUFFER
      handler.writeRequested(ctx, me)

      there was no(c).close()
    }

    "terminate chunked HTTP/1.1 responses with connect: close" in {
      val request = makeRequest(HttpVersion.HTTP_1_1, "connection" -> "close")
      val response = makeResponse(HttpVersion.HTTP_1_1)
      response.setChunked(true)

      perform(request, response)
      there was no(c).close()
      cFuture.setSuccess()   // write success
      there was no(c).close()

      val chunk = new DefaultHttpChunk(
        ChannelBuffers.copiedBuffer("content", Charset.forName("UTF-8")))

      me.getMessage returns chunk
      handler.writeRequested(ctx, me)
      me.getMessage returns chunk
      handler.writeRequested(ctx, me)

      // The final chunk.
      me.getMessage returns new DefaultHttpChunkTrailer
      handler.writeRequested(ctx, me)

      there was one(c).close()
    }
  }
}

