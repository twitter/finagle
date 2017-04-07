package com.twitter.finagle.netty4.channel

import io.netty.buffer.{ByteBuf, ByteBufHolder, ByteBufUtil, DefaultByteBufHolder, EmptyByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, OneInstancePerTest}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class AnyToHeapInboundHandlerTest extends FunSuite
  with GeneratorDrivenPropertyChecks
  with OneInstancePerTest {

  val channel = new EmbeddedChannel(AnyToHeapInboundHandler)

  // Generates random ByteBufs with:
  //  - Capacity: [1..100]
  //  - Write-Index: [0..Capacity]
  //  - Read-Index: [0..Write-Index]
  def genBuffer: Gen[ByteBuf] = for {
    capacity <- Gen.choose(1, 100)
    bytes <- Gen.listOfN(capacity, Arbitrary.arbByte.arbitrary)
    writer <- Gen.choose(0, capacity)
    reader <- Gen.choose(0, writer)
  } yield {
    Unpooled.buffer(capacity).setBytes(0, bytes.toArray)
      .writerIndex(writer)
      .readerIndex(reader)
  }

  test("convert to heap") {
    def assertDirectIsCopied(in: ByteBuf, out: ByteBuf): Unit = {
      // The output buffer should never be direct (unless it's an `EmptyByteBuf`).
      assert(out.isInstanceOf[EmptyByteBuf] || !out.isDirect)
      // The output buffer should be equal to input.
      assert(ByteBufUtil.equals(in, out))
      // The input buffer should've been released.
      assert(in.release())
    }

    forAll(genBuffer) { in: ByteBuf =>
      // We need to retain-duplicate so we can check the equality on the input
      // buffer after its being released by DirectToHeap.
      channel.writeInbound(in.retainedDuplicate())
      val out = channel.readInbound[ByteBuf]

      assertDirectIsCopied(in, out)
    }

    forAll(genBuffer) { in: ByteBuf =>
      // We need to retain-duplicate so we can check the equality on the input
      // buffer after its being released by DirectToHeap.
      channel.writeInbound(new DefaultByteBufHolder(in.retainedDuplicate()))
      val out = channel.readInbound[ByteBufHolder]

      assertDirectIsCopied(in, out.content)
    }
  }

  test("bypass non-ByteBufs/non-ByteBufHolders") {
    channel.writeInbound("foo")
    assert(channel.readInbound[String] == "foo")
  }

  test("bypass empty byte bufs") {
    channel.writeInbound(Unpooled.EMPTY_BUFFER)
    assert(channel.readInbound[ByteBuf] eq Unpooled.EMPTY_BUFFER)
  }

  test("bypass empty byte buf holders") {
    val in = new DefaultByteBufHolder(Unpooled.EMPTY_BUFFER)
    channel.writeInbound(in)
    assert(channel.readInbound[ByteBufHolder] eq in)
  }

  test("map unreadable bufs into empty bufs") {
    val in = Unpooled.directBuffer().retainedSlice(0, 0)
    channel.writeInbound(in)
    assert(channel.readInbound[ByteBuf] eq Unpooled.EMPTY_BUFFER)
    assert(in.release())
  }

  test("map unreadable buf holders into empty buf holders") {
    val in = new DefaultByteBufHolder(Unpooled.directBuffer().retainedSlice(0, 0))
    channel.writeInbound(in)
    assert(channel.readInbound[ByteBufHolder].content() eq Unpooled.EMPTY_BUFFER)
    assert(in.release())
  }
}

