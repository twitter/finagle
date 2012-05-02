package com.twitter.finagle.http.system

import com.twitter.finagle.Service
import com.twitter.finagle.http.Response
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, DefaultHttpResponse, HttpVersion,
HttpResponseStatus}
import org.specs.Specification
import util.Random
import org.specs.mock.Mockito

object HttpMuxServiceSpec extends Specification with Mockito {
  // todo: add other metrics when they are supported
  "Default Http server" should {
    class DummyService(reply: String) extends Service[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest): Future[HttpResponse] = {
        val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        response.setContent(ChannelBuffers.wrappedBuffer(reply.getBytes("UTF-8")))
        Future.value(response)
      }
    }

    val (reply1, reply2, reply3) = ("dumb", "dumber", "dumbest")

    val muxService = new HttpMuxService()
      .withHandler("foo/bar/", new DummyService(reply1)) // prefix match
      .withHandler("foo/bar", new DummyService(reply2))  // exact match
      .withHandler("foo/boo/baz/", new DummyService(reply3))

    "prefix matching request path correctly" in {
      val request = mock[HttpRequest]
      request.getUri() returns "/fooblah"
      Response(muxService(request)()).status must be_==(HttpResponseStatus.NOT_FOUND)

      request.getUri() returns "/foo/bar/blah"
      Response(muxService(request)()).contentString must be_==(reply1)

      // after normalization, it should match "/foo/bar/"
      request.getUri() returns "/foo//bar/blah"
      Response(muxService(request)()).contentString must be_==(reply1)

      request.getUri() returns "/foo/bar"
      Response(muxService(request)()).contentString must be_==(reply2)

      request.getUri() returns "/foo/bar/"
      Response(muxService(request)()).contentString must be_==(reply1)

      request.getUri() returns "/foo/boo/baz"
      Response(muxService(request)()).status must be_==(HttpResponseStatus.NOT_FOUND)

      request.getUri() returns "/foo/boo/baz/blah"
      Response(muxService(request)()).contentString must be_==(reply3)

      request.getUri() returns "/foo/barblah"
      Response(muxService(request)()).status must be_==(HttpResponseStatus.NOT_FOUND)
    }

    "Registering a service with an existing name will overwrite the old" in {
      val request = mock[HttpRequest]

      val (r1, r2, r3) = ("smart", "smarter", "smartest")
      val mux2 = muxService
        .withHandler("foo/bar/", new DummyService(r1)) // prefix match
        .withHandler("foo/bar", new DummyService(r2))  // exact match
        .withHandler("foo/boo/baz/", new DummyService(r3))

      request.getUri() returns "/fooblah"
      Response(mux2(request)()).status must be_==(HttpResponseStatus.NOT_FOUND)

      request.getUri() returns "/foo/bar/blah"
      Response(mux2(request)()).contentString must be_==(r1)

      request.getUri() returns "/foo/bar"
      Response(mux2(request)()).contentString must be_==(r2)

      request.getUri() returns "/foo/bar/"
      Response(mux2(request)()).contentString must be_==(r1)

      request.getUri() returns "/foo/boo/baz"
      Response(mux2(request)()).status must be_==(HttpResponseStatus.NOT_FOUND)

      request.getUri() returns "/foo/boo/baz/blah"
      Response(mux2(request)()).contentString must be_==(r3)

      request.getUri() returns "/foo/barblah"
      Response(mux2(request)()).status must be_==(HttpResponseStatus.NOT_FOUND)
    }
  }
}
