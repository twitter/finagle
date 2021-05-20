package com.twitter.finagle.netty4.http.handler

import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.DecoderResult
import io.netty.handler.codec.http.{DefaultHttpContent, HttpContent}
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class ClientExceptionMapperTest extends AnyFunSuite with OneInstancePerTest {

  val embed: EmbeddedChannel = new EmbeddedChannel(ClientExceptionMapper)

  test("convert failed decoder result") {
    val boom = new Exception("boom")
    val o = new DefaultHttpContent(Unpooled.wrappedBuffer("zoom".getBytes("UTF-8")))
    o.setDecoderResult(DecoderResult.failure(boom))

    assert(o.refCnt() == 1)
    assert(intercept[Exception](embed.writeInbound(o)) == boom)
    assert(o.refCnt() == 0)
    assert(!embed.finish())
  }

  test("bypass successful decoder result") {
    val o = new DefaultHttpContent(Unpooled.EMPTY_BUFFER)

    assert(embed.writeInbound(o))
    assert(o eq embed.readInbound[HttpContent]())
    assert(!embed.finish())
  }
}
