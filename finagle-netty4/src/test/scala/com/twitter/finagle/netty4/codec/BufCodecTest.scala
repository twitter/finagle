package com.twitter.finagle.netty4.codec

import com.twitter.finagle.Failure
import com.twitter.finagle.netty4.ByteBufAsBuf
import com.twitter.io.Buf
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel
import java.nio.ByteBuffer
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import org.junit.runner.RunWith
import org.scalacheck.{Arbitrary, Gen}
import org.scalatest.{FunSuite, OneInstancePerTest}
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class BufCodecTest extends FunSuite
  with GeneratorDrivenPropertyChecks
  with OneInstancePerTest {

  val channel = new EmbeddedChannel(BufCodec)

  test("fail to decode non-ByteBuf") {
    intercept[Failure] { channel.writeInbound("foo") }
    assert(!channel.finish())
  }

  test("fail to encode non-Buf") {
    assert(channel.write("foo").cause.isInstanceOf[Failure])
    assert(!channel.finish())
  }

  test("decode") {
    assert(channel.writeInbound(Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.UTF_8))))
    assert(channel.readInbound[Buf]() == Buf.Utf8("hello"))
    assert(!channel.finish())
  }

  def genRegularBuf: Gen[Buf] = for {
    capacity <- Gen.choose(1, 100)
    bytes <- Gen.listOfN(capacity, Arbitrary.arbByte.arbitrary)
    begin <- Gen.choose(0, capacity)
    end <- Gen.choose(begin, capacity)
    result <- Gen.oneOf(
      Buf.ByteArray.Owned(bytes.toArray, begin, end),
      Buf.ByteBuffer.Owned(ByteBuffer.wrap(bytes.toArray, begin, end - begin)),
      ByteBufAsBuf.Owned(Unpooled.wrappedBuffer(bytes.toArray, begin, end - begin))
    )
  } yield result

  def genCompositeBuf: Gen[Buf] = for {
    length <- Gen.choose(1, 5)
    bufs <- Gen.listOfN(length, genRegularBuf)
  } yield Buf(bufs)

  def genBuf: Gen[Buf] = Gen.oneOf(genCompositeBuf, genRegularBuf)

  test("encode composite/byte-array") {
    forAll(genBuf) { in: Buf =>
      assert(channel.writeOutbound(in))
      assert(in == ByteBufAsBuf.Owned(channel.readOutbound[ByteBuf]()))
    }

    assert(!channel.finish())
  }

  test("bypass direct ByteBufs") {
    val bb = Unpooled.directBuffer(3).writeBytes("foo".getBytes("UTF-8"))
    assert(channel.writeOutbound(ByteBufAsBuf.Owned(bb)))
    assert(channel.readOutbound[ByteBuf]() eq bb)
    assert(!channel.finish())
  }

  test("bypass direct ByteBuffers") {
    val bb = ByteBuffer.allocateDirect(3).put("foo".getBytes("UTF-8"))
    assert(channel.writeOutbound(Buf.ByteBuffer.Owned(bb)))
    assert(channel.readOutbound[ByteBuf]().nioBuffer().compareTo(bb) == 0)
    assert(!channel.finish())
  }
}
