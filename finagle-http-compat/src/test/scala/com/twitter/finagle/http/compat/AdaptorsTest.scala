package com.twitter.finagle.http.compat

import com.twitter.finagle.http.netty.Bijections
import com.twitter.finagle.http.{Fields, Request, Response, Method, Status, Version}
import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.io.{Buf, BufReader, Reader}
import com.twitter.util.Await
import java.net.{InetAddress, InetSocketAddress, URI}
import org.jboss.netty.handler.codec.http.{HttpHeaders, HttpRequest, HttpResponse}
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

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

  val arbResponse = for {
    code    <- Gen.chooseNum(100, 510)
    version <- Gen.oneOf(Version.Http10, Version.Http11)
    chunked <- arbitrary[Boolean]
    headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body    <- arbitrary[String]
  } yield {
    if (chunked) {
      val res = Response(version, Status(code), Reader.fromBuf(Buf.Utf8(body)))
      headers foreach { case (k, v) => res.headerMap.add(k, v) }
      res.headerMap.set(Fields.TransferEncoding, "chunked")
      (res, body)
    } else {
      val res = Response(version, Status(code))
      headers foreach { case (k, v) => res.headerMap.add(k, v) }
      res.contentString = body
      (res, body)
    }
  }

  val arbRequest = for {
    method  <- arbMethod
    uri     <- arbUri
    version <- Gen.oneOf(Version.Http10, Version.Http11)
    chunked <- arbitrary[Boolean]
    headers <- Gen.containerOf[Seq, (String, String)](arbHeader)
    body    <- arbitrary[String]
  } yield {
    val reqIn = Request(version, method, uri)
    headers foreach { case (k, v) => reqIn.headers.add(k, v) }
    val req = Request(
      reqIn.httpRequest,
      BufReader(Buf.Utf8(body)),
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    if (chunked) {
      req.headers.set(Fields.TransferEncoding, "chunked")
      req.setChunked(chunked)
    } else req.contentString = body
    (req, body)
  }

  val arbNettyResponse =
    arbResponse map { case (r: Response, body: String) =>
      (r.httpResponse, body)
    }

  val arbNettyRequest =
    arbRequest map { case (r: Request, body: String) =>
      (r.httpRequest, body)
    }

  test("netty: http request to netty") {
    forAll(arbRequest) { case (in: Request, body: String) =>
      if (in.isChunked) {
        val exc = intercept[Exception] { Await.result(NettyAdaptor.in(in)) }
        assert(NettyAdaptor.NoStreaming == exc)
      } else {
        val out = Await.result(NettyAdaptor.in(in))
        assert(out.getProtocolVersion == from(in.version))
        assert(out.getMethod == from(in.method))
        assert(out.getUri == in.getUri)
        assert(out.headers == in.headers)
        assert(out.isChunked == in.isChunked)
        assert(out.getContent == in.getContent)
      }
    }
  }

  test("netty: netty response to http") {
    forAll(arbNettyResponse) { case (in: HttpResponse, body: String) =>
      if (in.isChunked) {
        val exc = intercept[Exception] { Await.result(NettyAdaptor.out(in)) }
        assert(NettyAdaptor.NoStreaming == exc)
      } else {
        val out = Await.result(NettyAdaptor.out(in))
        assert(out.version == from(in.getProtocolVersion))
        assert(out.status == from(in.getStatus))
        assert(out.headers == in.headers)
        assert(out.isChunked == in.isChunked)
        assert(out.getContent == in.getContent)
      }
    }
  }

  test("http: netty request to http") {
    forAll(arbNettyRequest) { case (in: HttpRequest, body: String) =>
      if (in.isChunked) {
        val exc = intercept[Exception] { Await.result(NettyClientAdaptor.in(in)) }
        assert(NettyClientAdaptor.NoStreaming == exc)
      } else {
        val out = Await.result(NettyClientAdaptor.in(in))
        assert(out.version == from(in.getProtocolVersion))
        assert(out.method == from(in.getMethod))
        assert(out.getUri == in.getUri)
        assert(out.headers == in.headers)
        assert(out.isChunked == in.isChunked)
        assert(out.getContent == in.getContent)
      }
    }
  }

  test("http: http response to netty") {
    forAll(arbResponse) { case (in: Response, body: String) =>
      if (in.isChunked) {
        val exc = intercept[Exception] { Await.result(NettyClientAdaptor.out(in)) }
        assert(NettyClientAdaptor.NoStreaming == exc)
      } else {
        val out = Await.result(NettyClientAdaptor.out(in))
        assert(out.getProtocolVersion == from(in.version))
        assert(out.getStatus == from(in.status))
        assert(out.headers == in.headers)
        assert(out.isChunked == in.isChunked)
        assert(out.getContent == BufChannelBuffer(in.content))
      }
    }
  }
}
