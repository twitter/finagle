package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{DefaultHttpResponse, HttpRequest,
  HttpResponse, HttpResponseStatus, HttpVersion}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class HttpMuxerTest extends FunSuite {
  // todo: add other metrics when they are supported
  class DummyService(reply: String) extends Service[HttpRequest, HttpResponse] {
    def apply(request: HttpRequest): Future[HttpResponse] = {
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      response.setContent(ChannelBuffers.wrappedBuffer(reply.getBytes("UTF-8")))
      Future.value(response)
    }
  }

  val (reply1, reply2, reply3) = ("dumb", "dumber", "dumbest")

  val muxService = new HttpMuxer()
    .withHandler("foo/bar/", new DummyService(reply1)) // prefix match
    .withHandler("foo/bar", new DummyService(reply2))  // exact match
    .withHandler("foo/boo/baz/", new DummyService(reply3))

  test("handles params properly") {
    assert(Response(muxService(Request("/foo/bar/blah?j={}"))()).contentString == reply1)
  }

  test("prefix matching request path correctly") {
    assert(Response(muxService(Request("/fooblah"))()).status == HttpResponseStatus.NOT_FOUND)

    assert(Response(muxService(Request("/foo/bar/blah"))()).contentString == reply1)

    // after normalization, it should match "/foo/bar/"
    assert(Response(muxService(Request("/foo//bar/blah"))()).contentString == reply1)

    assert(Response(muxService(Request("/foo/bar"))()).contentString == reply2)

    assert(Response(muxService(Request("/foo/bar/"))()).contentString == reply1)

    assert(Response(muxService(Request("/foo/boo/baz"))()).status == HttpResponseStatus.NOT_FOUND)

    assert(Response(muxService(Request("/foo/boo/baz/blah"))()).contentString == reply3)

    assert(Response(muxService(Request("/foo/barblah"))()).status == HttpResponseStatus.NOT_FOUND)
  }

  test("Registering a service with an existing name will overwrite the old") {
    val (r1, r2, r3) = ("smart", "smarter", "smartest")
    val mux2 = muxService
      .withHandler("foo/bar/", new DummyService(r1)) // prefix match
      .withHandler("foo/bar", new DummyService(r2))  // exact match
      .withHandler("foo/boo/baz/", new DummyService(r3))

    assert(Response(mux2(Request("/fooblah"))()).status == HttpResponseStatus.NOT_FOUND)

    assert(Response(mux2(Request("/foo/bar/blah"))()).contentString == r1)

    assert(Response(mux2(Request("/foo/bar"))()).contentString == r2)

    assert(Response(mux2(Request("/foo/bar/"))()).contentString == r1)

    assert(Response(mux2(Request("/foo/boo/baz"))()).status == HttpResponseStatus.NOT_FOUND)

    assert(Response(mux2(Request("/foo/boo/baz/blah"))()).contentString == r3)

    assert(Response(mux2(Request("/foo/barblah"))()).status == HttpResponseStatus.NOT_FOUND)
  }
}
