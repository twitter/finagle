package com.twitter.finagle.http

import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.{HttpRequest=>HttpAsk, _}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HttpMuxerTest extends FunSuite {
  // todo: add other metrics when they are supported
  class DummyService(reply: String) extends Service[HttpAsk, HttpResponse] {
    def apply(request: HttpAsk): Future[HttpResponse] = {
      val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      response.setContent(ChannelBuffers.wrappedBuffer(reply.getBytes("UTF-8")))
      Future.value(response)
    }
  }

  val (fooBarPrefix, fooBarExact, fooBooBaz, exactMatch, specialCase) =
      ("fooBarPrefix", "fooBarExact", "fooBooBaz", "exactMatch", "specialCase")

  val muxService = new HttpMuxer()
    .withHandler("foo/bar/", new DummyService(fooBarPrefix)) // prefix match
    .withHandler("foo/bar", new DummyService(fooBarExact))  // exact match -- not shadowed by foo/bar/
    .withHandler("foo/boo/baz/", new DummyService(fooBooBaz))
    .withHandler("exact/match", new DummyService(exactMatch)) // exact match

  test("handles params properly") {
    assert(Response(Await.result(muxService(Ask("/foo/bar/blah?j={}")))).contentString === fooBarPrefix)
  }

  test("prefix matching is handled correctly") {
    assert(Response(Await.result(muxService(Ask("/fooblah")))).status === HttpResponseStatus.NOT_FOUND)

    assert(Response(Await.result(muxService(Ask("/foo/bar/blah")))).contentString === fooBarPrefix)

    assert(Response(Await.result(muxService(Ask("/foo//bar/blah")))).contentString === fooBarPrefix)

    assert(Response(Await.result(muxService(Ask("/foo/bar")))).contentString === fooBarExact)

    assert(Response(Await.result(muxService(Ask("/foo/bar/")))).contentString === fooBarPrefix)

    assert(Response(Await.result(muxService(Ask("/foo/boo/baz")))).status === HttpResponseStatus.NOT_FOUND)

    assert(Response(Await.result(muxService(Ask("/foo/boo/baz/blah")))).contentString === fooBooBaz)

    assert(Response(Await.result(muxService(Ask("/foo/barblah")))).status === HttpResponseStatus.NOT_FOUND)
  }

  test("exact matching is handled correctly") {
    assert(Response(Await.result(muxService(Ask("/exact/match")))).contentString === exactMatch)

    assert(Response(Await.result(muxService(Ask("/exact/match/")))).status === HttpResponseStatus.NOT_FOUND)

    assert(Response(Await.result(muxService(Ask("/exact/match/nested")))).status === HttpResponseStatus.NOT_FOUND)
  }

  test("""special cases "" and "/" are handled correctly""") {
    val slashMux = new HttpMuxer().withHandler("/", new DummyService(specialCase))
    assert(Response(Await.result(slashMux(Ask("/")))).contentString === specialCase)
    assert(Response(Await.result(slashMux(Ask("")))).status === HttpResponseStatus.NOT_FOUND)
    assert(Response(Await.result(slashMux(Ask("/anything")))).contentString === specialCase)

    val emptyStringMux = new HttpMuxer().withHandler("", new DummyService(specialCase))
    assert(Response(Await.result(emptyStringMux(Ask("/")))).contentString === specialCase)
    assert(Response(Await.result(emptyStringMux(Ask("")))).contentString === specialCase)
    assert(Response(Await.result(emptyStringMux(Ask("/anything")))).status === HttpResponseStatus.NOT_FOUND)
  }

  test("Registering a service with an existing name will overwrite the old") {
    val (origResp, newResp) = ("orig", "new")
    val orig = new HttpMuxer().withHandler("foo/", new DummyService(origResp))
    val overridden = orig.withHandler("foo/", new DummyService(newResp))

    assert(Response(Await.result(orig(Ask("/foo/bar")))).contentString === origResp)
    assert(Response(Await.result(overridden(Ask("/foo/bar")))).contentString === newResp)
  }
}
