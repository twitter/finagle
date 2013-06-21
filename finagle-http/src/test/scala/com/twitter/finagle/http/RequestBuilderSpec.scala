package com.twitter.finagle.http

import org.specs.SpecificationWithJUnit
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import java.net.URL
import scala.collection.JavaConverters._

class RequestBuilderSpec extends SpecificationWithJUnit {
  val URL0 = new URL("http://joe:blow@www.google.com:77/xxx?foo=bar#xxx")
  val URL1 = new URL("https://www.google.com/")

  val BODY0 = copiedBuffer("blah".getBytes)
  val FORM0 = Seq (
    "k1" -> "v1",
    "k2" -> "v2",
    "k3" -> "v3"
  )

  val MULTIPART0 =
"""--Boundary
Content-Disposition: form-data; name="k1"
Content-Type: text/plain; charset=UTF-8

v1
--Boundary
Content-Disposition: form-data; name="k2"
Content-Type: text/plain; charset=UTF-8

v2
--Boundary
Content-Disposition: form-data; name="k3"
Content-Type: text/plain; charset=UTF-8

v3
--Boundary--
""".replace("\r\n", "\n")

  val FORMPOST0 = "k1=v1&k2=v2&k3=v3"

  "RequestBuilder" should {
    "reject non-http urls" in {
      RequestBuilder().url(new URL("ftp:///blah")) must throwAn[IllegalArgumentException]
    }

    "set the HOST header" in {
      val get = RequestBuilder().url(URL0).buildGet
      val head = RequestBuilder().url(URL0).buildHead
      val delete = RequestBuilder().url(URL0).buildDelete
      val put = RequestBuilder().url(URL0).buildPut(BODY0)
      val post = RequestBuilder().url(URL0).buildPost(BODY0)

      val expected = "www.google.com:77"
      get.getHeader(HttpHeaders.Names.HOST) must_== expected
      head.getHeader(HttpHeaders.Names.HOST) must_== expected
      delete.getHeader(HttpHeaders.Names.HOST) must_== expected
      put.getHeader(HttpHeaders.Names.HOST) must_== expected
      post.getHeader(HttpHeaders.Names.HOST) must_== expected
    }

    "set the Authorization header when userInfo is present" in {
      val req0 = RequestBuilder().url(URL0).buildGet
      val req1 = RequestBuilder().url(URL1).buildGet
      val req2 = RequestBuilder().url(URL0).buildHead
      val req3 = RequestBuilder().url(URL1).buildHead
      val req8 = RequestBuilder().url(URL0).buildDelete
      val req9 = RequestBuilder().url(URL1).buildDelete
      val req4 = RequestBuilder().url(URL0).buildPut(BODY0)
      val req5 = RequestBuilder().url(URL1).buildPut(BODY0)
      val req6 = RequestBuilder().url(URL0).buildPost(BODY0)
      val req7 = RequestBuilder().url(URL1).buildPost(BODY0)

      val expected = "Basic am9lOmJsb3c="
      req0.getHeader(HttpHeaders.Names.AUTHORIZATION) must_== expected
      req1.getHeader(HttpHeaders.Names.AUTHORIZATION) must beNull
      req2.getHeader(HttpHeaders.Names.AUTHORIZATION) must_== expected
      req3.getHeader(HttpHeaders.Names.AUTHORIZATION) must beNull
      req8.getHeader(HttpHeaders.Names.AUTHORIZATION) must_== expected
      req9.getHeader(HttpHeaders.Names.AUTHORIZATION) must beNull
      req4.getHeader(HttpHeaders.Names.AUTHORIZATION) must_== expected
      req5.getHeader(HttpHeaders.Names.AUTHORIZATION) must beNull
      req6.getHeader(HttpHeaders.Names.AUTHORIZATION) must_== expected
      req7.getHeader(HttpHeaders.Names.AUTHORIZATION) must beNull
    }

    "replace the empty path with /" in {
      val req0 = RequestBuilder().url(new URL("http://a.com")).buildGet
      val req1 = RequestBuilder().url(new URL("http://a.com?xxx")).buildGet

      req0.getUri must_== "/"
      req1.getUri must_== "/?xxx"
    }

    "not include the fragment in the resource" in {
      val u0 = new URL("http://a.com#xxx")
      val u1 = new URL("http://a.com/#xxx")
      val u2 = new URL("http://a.com/?abc=def#xxx")

      val get0 = RequestBuilder().url(u0).buildGet
      val get1 = RequestBuilder().url(u1).buildGet
      val get2 = RequestBuilder().url(u2).buildGet
      val head0 = RequestBuilder().url(u0).buildHead
      val head1 = RequestBuilder().url(u1).buildHead
      val head2 = RequestBuilder().url(u2).buildHead
      val del0 = RequestBuilder().url(u0).buildDelete
      val del1 = RequestBuilder().url(u1).buildDelete
      val del2 = RequestBuilder().url(u2).buildDelete
      val put0 = RequestBuilder().url(u0).buildPut(BODY0)
      val put1 = RequestBuilder().url(u1).buildPut(BODY0)
      val put2 = RequestBuilder().url(u2).buildPut(BODY0)
      val post0 = RequestBuilder().url(u0).buildPost(BODY0)
      val post1 = RequestBuilder().url(u1).buildPost(BODY0)
      val post2 = RequestBuilder().url(u2).buildPost(BODY0)

      get0.getUri must_== "/"
      get1.getUri must_== "/"
      get2.getUri must_== "/?abc=def"
      head0.getUri must_== "/"
      head1.getUri must_== "/"
      head2.getUri must_== "/?abc=def"
      del0.getUri must_== "/"
      del1.getUri must_== "/"
      del2.getUri must_== "/?abc=def"
      put0.getUri must_== "/"
      put1.getUri must_== "/"
      put2.getUri must_== "/?abc=def"
      post0.getUri must_== "/"
      post1.getUri must_== "/"
      post2.getUri must_== "/?abc=def"
    }

    "not include the empty query string" in {
      val u0 = new URL("http://a.com?")
      val u1 = new URL("http://a.com/?")
      val u2 = new URL("http://a.com/?#xxx")

      val get0 = RequestBuilder().url(u0).buildGet
      val get1 = RequestBuilder().url(u1).buildGet
      val get2 = RequestBuilder().url(u2).buildGet
      val head0 = RequestBuilder().url(u0).buildHead
      val head1 = RequestBuilder().url(u1).buildHead
      val head2 = RequestBuilder().url(u2).buildHead
      val del0 = RequestBuilder().url(u0).buildDelete
      val del1 = RequestBuilder().url(u1).buildDelete
      val del2 = RequestBuilder().url(u2).buildDelete
      val put0 = RequestBuilder().url(u0).buildPut(BODY0)
      val put1 = RequestBuilder().url(u1).buildPut(BODY0)
      val put2 = RequestBuilder().url(u2).buildPut(BODY0)
      val post0 = RequestBuilder().url(u0).buildPost(BODY0)
      val post1 = RequestBuilder().url(u1).buildPost(BODY0)
      val post2 = RequestBuilder().url(u2).buildPost(BODY0)

      get0.getUri must_== "/"
      get1.getUri must_== "/"
      get2.getUri must_== "/"
      head0.getUri must_== "/"
      head1.getUri must_== "/"
      head2.getUri must_== "/"
      del0.getUri must_== "/"
      del1.getUri must_== "/"
      del2.getUri must_== "/"
      put0.getUri must_== "/"
      put1.getUri must_== "/"
      put2.getUri must_== "/"
      post0.getUri must_== "/"
      post1.getUri must_== "/"
      post2.getUri must_== "/"
    }

    "set the correct protocol version" in {
      val get0 = RequestBuilder().url(URL0).buildGet
      val get1 = RequestBuilder().url(URL0).http10.buildGet
      val head0 = RequestBuilder().url(URL0).buildHead
      val head1 = RequestBuilder().url(URL0).http10.buildHead
      val del0 = RequestBuilder().url(URL0).buildDelete
      val del1 = RequestBuilder().url(URL0).http10.buildDelete
      val put0 = RequestBuilder().url(URL0).buildPut(BODY0)
      val put1 = RequestBuilder().url(URL0).http10.buildPut(BODY0)
      val post0 = RequestBuilder().url(URL0).buildPost(BODY0)
      val post1 = RequestBuilder().url(URL0).http10.buildPost(BODY0)

      get0.getProtocolVersion must_== HttpVersion.HTTP_1_1
      get1.getProtocolVersion must_== HttpVersion.HTTP_1_0
      head0.getProtocolVersion must_== HttpVersion.HTTP_1_1
      head1.getProtocolVersion must_== HttpVersion.HTTP_1_0
      del0.getProtocolVersion must_== HttpVersion.HTTP_1_1
      del1.getProtocolVersion must_== HttpVersion.HTTP_1_0
      put0.getProtocolVersion must_== HttpVersion.HTTP_1_1
      put1.getProtocolVersion must_== HttpVersion.HTTP_1_0
      post0.getProtocolVersion must_== HttpVersion.HTTP_1_1
      post1.getProtocolVersion must_== HttpVersion.HTTP_1_0
    }

    "set the correct method" in {
      val get = RequestBuilder().url(URL0).buildGet
      val head = RequestBuilder().url(URL0).buildHead
      val del = RequestBuilder().url(URL0).buildDelete
      val put = RequestBuilder().url(URL0).buildPut(BODY0)
      val post = RequestBuilder().url(URL0).buildPost(BODY0)

      get.getMethod must_== HttpMethod.GET
      head.getMethod must_== HttpMethod.HEAD
      del.getMethod must_== HttpMethod.DELETE
      put.getMethod must_== HttpMethod.PUT
      post.getMethod must_== HttpMethod.POST
    }

    "set headers" in {
      val A = "A"
      val B = "B"
      val C = "C"
      val D = "D"
      val builder0 = RequestBuilder()
        .url(URL0)
        .setHeader(A, B)

      builder0.buildGet.getHeader(A) must_== B
      builder0.buildHead.getHeader(A) must_== B
      builder0.buildDelete.getHeader(A) must_== B
      builder0.buildPut(BODY0).getHeader(A) must_== B
      builder0.buildPost(BODY0).getHeader(A) must_== B

      val builder1 = builder0
        .setHeader(A, C)

      builder1.buildGet.getHeader(A) must_== C
      builder1.buildHead.getHeader(A) must_== C
      builder1.buildDelete.getHeader(A) must_== C
      builder1.buildPut(BODY0).getHeader(A) must_== C
      builder1.buildPost(BODY0).getHeader(A) must_== C

      val builder2 = builder1
        .setHeader(A, Seq())

      builder2.buildGet.getHeader(A) must beNull
      builder2.buildHead.getHeader(A) must beNull
      builder2.buildDelete.getHeader(A) must beNull
      builder2.buildPut(BODY0).getHeader(A) must beNull
      builder2.buildPost(BODY0).getHeader(A) must beNull

      val builder3 = builder2
        .setHeader(A, Seq(B, C))

      val pair = Seq(B,C).asJava
      builder3.buildGet.getHeaders(A) must_== pair
      builder3.buildHead.getHeaders(A) must_== pair
      builder3.buildDelete.getHeaders(A) must_== pair
      builder3.buildPut(BODY0).getHeaders(A) must_== pair
      builder3.buildPost(BODY0).getHeaders(A) must_== pair

      val builder4 = builder3
        .addHeader(A, D)

      val triple = Seq(B,C, D).asJava
      builder4.buildGet.getHeaders(A) must_== triple
      builder4.buildHead.getHeaders(A) must_== triple
      builder4.buildDelete.getHeaders(A) must_== triple
      builder4.buildPut(BODY0).getHeaders(A) must_== triple
      builder4.buildPost(BODY0).getHeaders(A) must_== triple
    }

    "build form" in {
      val builder0 = RequestBuilder()
        .url(URL0)
        .addFormElement(FORM0:_*)

      val req0 = builder0.buildFormPost(false)
      val content = Request(req0).contentString.replace("\r\n", "\n")
      content must_== FORMPOST0
    }

    "build multipart form" in {
      val builder0 = RequestBuilder()
        .url(URL0)
        .addFormElement(FORM0:_*)

      val req0 = builder0.buildFormPost(true)
      val content = "--[^-\r\n]+".r.replaceAllIn(Request(req0).contentString, "--Boundary")
                    .replace("\r\n", "\n")
      content must_== MULTIPART0
    }
  }
}
