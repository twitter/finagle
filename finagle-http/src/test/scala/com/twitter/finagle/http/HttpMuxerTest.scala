package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.Method.Patch
import com.twitter.finagle.Service
import com.twitter.io.Buf
import com.twitter.util.{Await, Future, Time}
import org.scalatest.funsuite.AnyFunSuite

class HttpMuxerTest extends AnyFunSuite {
  def await[A](f: Future[A]): A = Await.result(f, 5.seconds)

  class DummyService(reply: String) extends Service[Request, Response] {
    def apply(request: Request): Future[Response] = {
      val response = Response(Version.Http11, Status.Ok)
      response.content = Buf.Utf8(reply)
      Future.value(response)
    }
  }

  val (fooBarPrefix, fooBarExact, fooBooBaz, exactMatch, specialCase, percentEncode) =
    ("fooBarPrefix", "fooBarExact", "fooBooBaz", "exactMatch", "specialCase", "percentEncode")

  val muxService = new HttpMuxer()
    .withHandler("foo/bar/", new DummyService(fooBarPrefix)) // prefix match
    .withHandler(
      "foo/bar",
      new DummyService(fooBarExact)
    ) // exact match -- not shadowed by foo/bar/
    .withHandler("foo/boo/baz/", new DummyService(fooBooBaz))
    .withHandler("exact/match", new DummyService(exactMatch)) // exact match
    .withHandler("foo/<a>", new DummyService(percentEncode))

  test("handles params properly") {
    assert(Await.result(muxService(Request("/foo/bar/blah?j={}"))).contentString == fooBarPrefix)
  }

  test("normalize basics") {
    assert(HttpMuxer.normalize("") == "")
    assert(HttpMuxer.normalize("/") == "/")
    assert(HttpMuxer.normalize("/foo") == "/foo")
    assert(HttpMuxer.normalize("/foo/") == "/foo/")
    assert(HttpMuxer.normalize("foo") == "/foo")
  }

  test("normalize duplicate slashes") {
    assert(HttpMuxer.normalize("////") == "/")
    assert(HttpMuxer.normalize("/foo//bar") == "/foo/bar")
    assert(HttpMuxer.normalize("///foo//bar") == "/foo/bar")
    assert(HttpMuxer.normalize("/foo//bar///") == "/foo/bar/")
    assert(HttpMuxer.normalize("foo////bar//") == "/foo/bar/")
  }

  test("prefix matching is handled correctly") {
    assert(await(muxService(Request("/foo/<a>"))).contentString == percentEncode)

    assert(await(muxService(Request("/fooblah"))).status == Status.NotFound)

    assert(await(muxService(Request("/foo/bar/blah"))).contentString == fooBarPrefix)

    assert(await(muxService(Request("/foo//bar/blah"))).contentString == fooBarPrefix)

    assert(await(muxService(Request("/foo//bar/<a>"))).contentString == fooBarPrefix)

    assert(await(muxService(Request("/foo/bar"))).contentString == fooBarExact)

    assert(await(muxService(Request("/foo/bar/"))).contentString == fooBarPrefix)

    assert(await(muxService(Request("/foo/boo/baz"))).status == Status.NotFound)

    assert(await(muxService(Request("/foo/boo/baz/blah"))).contentString == fooBooBaz)

    assert(await(muxService(Request("/foo/barblah"))).status == Status.NotFound)
  }

  test("exact matching is handled correctly") {
    assert(await(muxService(Request("/exact/match"))).contentString == exactMatch)

    assert(await(muxService(Request("/exact/match/"))).status == Status.NotFound)

    assert(await(muxService(Request("/exact/match/nested"))).status == Status.NotFound)
  }

  test("""special cases "" and "/" are handled correctly""") {
    val slashMux = new HttpMuxer().withHandler("/", new DummyService(specialCase))
    assert(await(slashMux(Request("/"))).contentString == specialCase)
    assert(await(slashMux(Request(""))).status == Status.NotFound)
    assert(await(slashMux(Request("/anything"))).contentString == specialCase)

    val emptyStringMux = new HttpMuxer().withHandler("", new DummyService(specialCase))
    assert(await(emptyStringMux(Request("/"))).contentString == specialCase)
    assert(await(emptyStringMux(Request(""))).contentString == specialCase)
    assert(await(emptyStringMux(Request("/anything"))).status == Status.NotFound)
  }

  test("Registering a service with an existing name will overwrite the old") {
    val (origResp, newResp) = ("orig", "new")
    val orig = new HttpMuxer().withHandler("foo/", new DummyService(origResp))
    val overridden = orig.withHandler("foo/", new DummyService(newResp))

    assert(await(orig(Request("/foo/bar"))).contentString == origResp)
    assert(await(overridden(Request("/foo/bar"))).contentString == newResp)
  }

  test("RouteIndex only allows GET and POST") {
    assertThrows[AssertionError] {
      val routeIndex = RouteIndex(
        alias = "foo",
        group = "bar",
        method = Patch
      )
    }
  }

  test("Closing the HttpMuxer closes the individual handlers") {
    var closed = false
    val closableSvc = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = ???
      override def close(deadline: Time): Future[Unit] = {
        closed = true
        Future.Done
      }
    }
    val closable = new HttpMuxer().withHandler("foo/", closableSvc)
    await(closable.close())
    assert(closed)
  }
}
