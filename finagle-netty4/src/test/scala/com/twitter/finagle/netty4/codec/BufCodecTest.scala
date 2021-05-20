package com.twitter.finagle.netty4.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.io.Buf
import io.netty.buffer.{ByteBuf, ByteBufUtil, EmptyByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import java.nio.ByteBuffer
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.OneInstancePerTest
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class BufCodecTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks with OneInstancePerTest {

  val channel = new EmbeddedChannel(BufCodec)

  def genArrayBeginAndEnd: Gen[(Array[Byte], Int, Int)] =
    for {
      capacity <- Gen.choose(1, 100)
      bytes <- Gen.listOfN(capacity, Arbitrary.arbByte.arbitrary)
      begin <- Gen.choose(0, capacity)
      end <- Gen.choose(begin, capacity)
    } yield (bytes.toArray, begin, end)

  def genRegularBuf: Gen[Buf] =
    for {
      (bytes, begin, end) <- genArrayBeginAndEnd
      result <- Gen.oneOf(
        Buf.ByteArray.Owned(bytes, begin, end),
        Buf.ByteBuffer.Owned(ByteBuffer.wrap(bytes, begin, end - begin)),
        ByteBufConversion.byteBufAsBuf(Unpooled.wrappedBuffer(bytes, begin, end - begin))
      )
    } yield result

  def genCompositeBuf: Gen[Buf] =
    for {
      length <- Gen.choose(1, 5)
      bufs <- Gen.listOfN(length, genRegularBuf)
    } yield Buf(bufs)

  def genBuf: Gen[Buf] = Gen.oneOf(genCompositeBuf, genRegularBuf)

  def genRegularByteBuf: Gen[ByteBuf] =
    for {
      (bytes, begin, end) <- genArrayBeginAndEnd
    } yield Unpooled.wrappedBuffer(bytes, begin, end - begin)

  def genCompositeByteBuf: Gen[ByteBuf] =
    for {
      length <- Gen.choose(1, 5)
      byteBufs <- Gen.listOfN(length, genRegularByteBuf)
    } yield Unpooled.wrappedBuffer(byteBufs: _*)

  def genByteBuf: Gen[ByteBuf] = Gen.oneOf(genRegularByteBuf, genCompositeByteBuf)

  test("fail to decode non-ByteBuf") {
    intercept[Failure] { channel.writeInbound("foo") }
    assert(!channel.finish())
  }

  test("fail to encode non-Buf") {
    assert(channel.write("foo").cause.isInstanceOf[Failure])
    assert(!channel.finish())
  }

  test("decode") {
    forAll(genByteBuf) { in =>
      assert(channel.writeInbound(in.retainedDuplicate()))
      assert(ByteBufUtil.equals(in, ByteBufConversion.bufAsByteBuf(channel.readInbound[Buf])))

      // BufCodec also makes sure to release the inbound buffer.
      assert(in.isInstanceOf[EmptyByteBuf] || in.release())
    }

    assert(!channel.finish())
  }

  test("encode") {
    forAll(genBuf) { in =>
      assert(channel.writeOutbound(in))
      assert(in == ByteBufConversion.byteBufAsBuf(channel.readOutbound[ByteBuf]()))
    }

    assert(!channel.finish())
  }

  test("bypass outbound direct ByteBuffers") {
    val bb = ByteBuffer.allocateDirect(3).put("foo".getBytes("UTF-8"))
    assert(channel.writeOutbound(Buf.ByteBuffer.Owned(bb)))
    assert(channel.readOutbound[ByteBuf]().nioBuffer().compareTo(bb) == 0)
    assert(!channel.finish())
  }
}
