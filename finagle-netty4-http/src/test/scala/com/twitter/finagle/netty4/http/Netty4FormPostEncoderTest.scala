package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.{Request, RequestConfig, SimpleElement}
import com.twitter.finagle.netty4.ByteBufAsBuf
import org.scalatest.FunSuite

class Netty4FormPostEncoderTest extends FunSuite {

  def encode(multipart: Boolean): Request = {
    val elements = Seq(
      "k1" -> "v",
      "k2" -> "v2",
      "k3" -> "v33"
    ).map { case (k, v) => SimpleElement(k, v) }

    val config = new RequestConfig(
      url = Some(new java.net.URL("http://localhost/")),
      formElements = elements
    )

    Netty4FormPostEncoder.encode(config, multipart)
  }

  test("Encoded form post requests are not backed by ByteBuf instances") {
    val req = encode(multipart = false)
    assert(!req.content.isInstanceOf[ByteBufAsBuf])
  }

  test("Encoded multipart form post requests are not backed by ByteBuf instances") {
    val req = encode(multipart = true)
    assert(!req.content.isInstanceOf[ByteBufAsBuf])
  }
}
