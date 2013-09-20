package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

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

  val (fooBarPrefix, fooBarExact, fooBooBaz, exactMatch, specialCase) =
      ("fooBarPrefix", "fooBarExact", "fooBooBaz", "exactMatch", "specialCase")

  val muxService = new HttpMuxer()
    .withHandler("foo/bar/", new DummyService(fooBarPrefix)) // prefix match
    .withHandler("foo/bar", new DummyService(fooBarExact))  // exact match -- shadowed by foo/bar/
    .withHandler("foo/boo/baz/", new DummyService(fooBooBaz))
    .withHandler("exact/match", new DummyService(exactMatch)) // exact match

  test("handles params properly") {
    assert(Response(Await.result(muxService(Request("/foo/bar/blah?j={}")))).contentString === fooBarPrefix)
  }

  test("prefix matching is handled correctly") {
    assert(Response(Await.result(muxService(Request("/fooblah")))).status === HttpResponseStatus.NOT_FOUND)

    assert(Response(Await.result(muxService(Request("/foo/bar/blah")))).contentString === fooBarPrefix)

    assert(Response(Await.result(muxService(Request("/foo//bar/blah")))).contentString === fooBarPrefix)

    assert(Response(Await.result(muxService(Request("/foo/bar")))).contentString === fooBarPrefix)

    assert(Response(Await.result(muxService(Request("/foo/bar/")))).contentString === fooBarPrefix)

    assert(Response(Await.result(muxService(Request("/foo/boo/baz")))).contentString === fooBooBaz)

    assert(Response(Await.result(muxService(Request("/foo/boo/baz/blah")))).contentString === fooBooBaz)

    assert(Response(Await.result(muxService(Request("/foo/barblah")))).status === HttpResponseStatus.NOT_FOUND)
  }

  test("exact matching is handled correctly") {
    assert(Response(Await.result(muxService(Request("/exact/match")))).contentString === exactMatch)

    assert(Response(Await.result(muxService(Request("/exact/match/")))).status === HttpResponseStatus.NOT_FOUND)

    assert(Response(Await.result(muxService(Request("/exact/match/nested")))).status === HttpResponseStatus.NOT_FOUND)
  }

  test("""special cases "" and "/" are handled correctly""") {
    val slashMux = new HttpMuxer().withHandler("/", new DummyService(specialCase))
    assert(Response(Await.result(slashMux(Request("/")))).contentString === specialCase)
    assert(Response(Await.result(slashMux(Request("")))).contentString === specialCase)
    assert(Response(Await.result(slashMux(Request("/anything")))).contentString === specialCase)

    val emptyStringMux = new HttpMuxer().withHandler("", new DummyService(specialCase))
    assert(Response(Await.result(emptyStringMux(Request("/")))).contentString === specialCase)
    assert(Response(Await.result(emptyStringMux(Request("")))).contentString === specialCase)
    assert(Response(Await.result(emptyStringMux(Request("/anything")))).status === HttpResponseStatus.NOT_FOUND)
  }

  test("Registering a service with an existing name will overwrite the old") {
    val (origResp, newResp) = ("orig", "new")
    val orig = new HttpMuxer().withHandler("foo/", new DummyService(origResp))
    val overridden = orig.withHandler("foo/", new DummyService(newResp))

    assert(Response(Await.result(orig(Request("/foo/bar")))).contentString === origResp)
    assert(Response(Await.result(overridden(Request("/foo/bar")))).contentString === newResp)
  }
}
