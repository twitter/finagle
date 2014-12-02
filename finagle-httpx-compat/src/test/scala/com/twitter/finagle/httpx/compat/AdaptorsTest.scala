package com.twitter.finagle.httpx.compat

import com.twitter.finagle.http
import com.twitter.finagle.httpx.{Fields, Request, Method, Version}
import com.twitter.finagle.httpx.netty.Bijections
import com.twitter.util.Await
import com.twitter.io.{Buf, BufReader, Reader}
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponseStatus, HttpResponse}
import java.net.{InetAddress, InetSocketAddress, URI}

@RunWith(classOf[JUnitRunner])
class FiltersTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import Arbitrary.arbitrary
  import Bijections._

  val arbMethod = Gen.oneOf(
    Method.Get, Method.Post, Method.Trace, Method.Delete, Method.Put,
    Method.Connect, Method.Options)

  val arbKeys = Gen.oneOf("Foo", "Bar", "Foo-Bar", "Bar-Baz")

  val arbUri = for {
    scheme  <- Gen.oneOf("http", "https")
    hostLen <- Gen.choose(1,20)
    pathLen <- Gen.choose(1,20)
    tld     <- Gen.oneOf(".net",".com", "org", ".edu")
    host = util.Random.alphanumeric.take(hostLen).mkString
    path = util.Random.alphanumeric.take(pathLen).mkString
  } yield (new URI(scheme, host + tld, "/" + path, null)).toASCIIString

  val arbHeader = for {
    key <- arbKeys
    len <- Gen.choose(0, 100)
  } yield (key, util.Random.alphanumeric.take(len).mkString)


  val arbRequest = for {
    method  <- arbMethod
    uri     <- arbUri
    version <- Gen.oneOf(Version.Http10, Version.Http11)
    chunked <- arbitrary[Boolean]
    headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body    <- arbitrary[String]
  } yield {
    val reqIn = Request(method, uri, version)
    headers foreach { case (k, v) => reqIn.headers.add(k, v) }
    val req = new Request {
      val httpRequest = reqIn.httpRequest
      override val reader = BufReader(Buf.Utf8(body))
      lazy val remoteSocketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    }
    if (chunked) {
      req.headers.set(Fields.TransferEncoding, "chunked")
      req.setChunked(chunked)
    } else req.contentString = body
    (req, body)
  }

  val arbHttpResponse = for {
    code    <- Gen.chooseNum(100, 510)
    version <- Gen.oneOf(http.Version.Http10, http.Version.Http11)
    chunked <- arbitrary[Boolean]
    headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body    <- arbitrary[String]
  } yield {
    val resIn = http.Response(version, HttpResponseStatus.valueOf(code))
    headers foreach { case (k, v) => resIn.headers.add(k, v) }
    val res = new http.Response {
      val httpResponse = resIn.httpResponse
      override val reader = BufReader(Buf.Utf8(body))
    }
    if (chunked) {
      res.headers.set(Fields.TransferEncoding, "chunked")
      res.setChunked(chunked)
    } else res.contentString = body
    (res, body)
  }

  val arbNettyResponse =
    arbHttpResponse map { case (r: http.Response, body: String) =>
      (r.httpResponse, body)
    }

  val arbNettyRequest =
    arbRequest map { case (r: Request, body: String) =>
      (r.httpRequest, body)
    }

  test("http: httpx request to http") {
    forAll(arbRequest) { case (in: Request, body: String) =>
      val out = Await.result(HttpAdaptor.in(in))
      assert(out.version === from(in.version))
      assert(out.method === from(in.method))
      assert(out.path === in.path)
      assert(out.headers === in.headers)
      assert(out.isChunked === in.isChunked)
      assert(out.getContent === in.getContent)
      val outBody = Await.result(Reader.readAll(out.reader))
      assert(outBody === Buf.Utf8(body))
    }
  }

  test("http: http response to httpx") {
    forAll(arbHttpResponse) { case (in: http.Response, body: String) =>
      val out = Await.result(HttpAdaptor.out(in))
      assert(out.version === from(in.version))
      assert(out.status === from(in.status))
      assert(out.headers === in.headers)
      assert(out.isChunked === in.isChunked)
      assert(out.getContent === in.getContent)
      val outBody = Await.result(Reader.readAll(out.reader))
      assert(outBody === Buf.Utf8(body))
    }
  }

  test("netty: httpx request to netty") {
    forAll(arbRequest) { case (in: Request, body: String) =>
      if (in.isChunked) {
        val exc = intercept[Exception] { Await.result(NettyAdaptor.in(in)) }
        assert(NettyAdaptor.NoStreaming === exc)
      } else {
        val out = Await.result(NettyAdaptor.in(in))
        assert(out.getProtocolVersion === from(in.version))
        assert(out.getMethod === from(in.method))
        assert(out.getUri === in.getUri)
        assert(out.headers === in.headers)
        assert(out.isChunked === in.isChunked)
        assert(out.getContent === in.getContent)
      }
    }
  }

  test("netty: netty response to httpx") {
    forAll(arbNettyResponse) { case (in: HttpResponse, body: String) =>
      if (in.isChunked) {
        val exc = intercept[Exception] { Await.result(NettyAdaptor.out(in)) }
        assert(NettyAdaptor.NoStreaming === exc)
      } else {
        val out = Await.result(NettyAdaptor.out(in))
        assert(out.version === from(in.getProtocolVersion))
        assert(out.status === from(in.getStatus))
        assert(out.headers === in.headers)
        assert(out.isChunked === in.isChunked)
        assert(out.getContent === in.getContent)
      }
    }
  }

  test("netty: netty request to httpx") {
    forAll(arbNettyRequest) { case (in: HttpRequest, body: String) =>
      if (in.isChunked) {
        val exc = intercept[Exception] { Await.result(NettyClientAdaptor.in(in)) }
        assert(NettyClientAdaptor.NoStreaming === exc)
      } else {
        val out = Await.result(NettyClientAdaptor.in(in))
        assert(out.version === from(in.getProtocolVersion))
        assert(out.method === from(in.getMethod))
        assert(out.getUri === in.getUri)
        assert(out.headers === in.headers)
        assert(out.isChunked === in.isChunked)
        assert(out.getContent === in.getContent)
      }
    }
  }
}
