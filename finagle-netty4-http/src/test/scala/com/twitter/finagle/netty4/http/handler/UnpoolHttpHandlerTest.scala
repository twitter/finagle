package com.twitter.finagle.netty4.http.handler

import io.netty.buffer._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import org.scalacheck.{Arbitrary, Gen}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.OneInstancePerTest
import org.scalatest.funsuite.AnyFunSuite

class UnpoolHttpHandlerTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with OneInstancePerTest {

  val channel = new EmbeddedChannel(UnpoolHttpHandler)

  // Generates random HTTP contents with:
  //  - Capacity: [1..100]
  //  - Write-Index: [0..Capacity]
  //  - Read-Index: [0..Write-Index]
  def genHttpContent: Gen[HttpContent] =
    for {
      capacity <- Gen.choose(1, 100)
      bytes <- Gen.listOfN(capacity, Arbitrary.arbByte.arbitrary)
      writer <- Gen.choose(0, capacity)
      reader <- Gen.choose(0, writer)
      content <- Gen.oneOf(
        Unpooled
          .buffer(capacity)
          .setBytes(0, bytes.toArray)
          .writerIndex(writer)
          .readerIndex(reader),
        Unpooled
          .directBuffer(capacity)
          .setBytes(0, bytes.toArray)
          .writerIndex(writer)
          .readerIndex(reader)
      )
      httpContent <- Gen.oneOf(
        new DefaultHttpContent(content),
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content),
        new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", content)
      )
    } yield httpContent

  test("convert to heap") {
    forAll(genHttpContent) { in: HttpContent =>
      // We need to retain-duplicate so we can check the equality on the input
      // buffer after its being released by DirectToHeap.
      channel.writeInbound(in.retainedDuplicate())
      val out = channel.readInbound[HttpContent]

      // The output buffer should never be direct (unless it's an `EmptyByteBuf`).
      assert(out.content.isInstanceOf[EmptyByteBuf] || !out.content.isDirect)
      // The output buffer should be equal to input.
      assert(!(in.content eq out.content) && ByteBufUtil.equals(in.content, out.content))
      // The input buffer should've been released.
      assert(in.release())
    }
  }

  test("bypass HTTP messages") {
    val msg = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    channel.writeInbound(msg)
    assert(channel.readInbound[HttpMessage] eq msg)
  }

  test("bypass empty HTTP contents") {
    channel.writeInbound(LastHttpContent.EMPTY_LAST_CONTENT)
    assert(channel.readInbound[HttpContent] eq LastHttpContent.EMPTY_LAST_CONTENT)
  }

  test("map unreadable buf holders into empty buf holders") {
    val in = new DefaultHttpContent(Unpooled.directBuffer().retainedSlice(0, 0))
    channel.writeInbound(in)
    assert(channel.readInbound[HttpContent].content eq Unpooled.EMPTY_BUFFER)
    assert(in.release())
  }
}
