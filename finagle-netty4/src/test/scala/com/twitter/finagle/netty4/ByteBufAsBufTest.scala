package com.twitter.finagle.netty4

import java.nio.CharBuffer

import com.twitter.io.Buf
import io.netty.buffer.Unpooled
import org.junit.runner.RunWith
import org.scalacheck.{Gen, Arbitrary}
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.scalatest.{OneInstancePerTest, FunSuite}

@RunWith(classOf[JUnitRunner])
class ByteBufAsBufTest
  extends FunSuite
  with GeneratorDrivenPropertyChecks
  with OneInstancePerTest {

  val bytes = Array[Byte](1,2,3,4)
  val underlying = Unpooled.buffer(100)
  underlying.writeBytes(bytes)
  val buf = new ByteBufAsBuf(underlying)

  test("ByteBufAsBuf.length equals underlying readable bytes") {
    assert(buf.length == 4)
    underlying.readByte()
    assert(buf.length == 3)
  }

  test("writes to underlying ByteBuf are reflected in containing ByteBufAsBuf") {
    assert(Buf.ByteArray.Owned(bytes) == buf)
    val newBytes = Array[Byte](10,20,30,40)
    underlying.writerIndex(0)
    underlying.writeBytes(newBytes)
    assert(Buf.ByteArray.Owned(newBytes) == buf)
  }

  test("writes to slices of the underlying ByteBuf are reflected in ByteBufAsBuf") {
    val bbSlice = underlying.slice(1,2)
    bbSlice.writerIndex(0)
    bbSlice.writeByte(99)
    bbSlice.writeByte(100)

    Buf.ByteArray.Owned.extract(buf).toSeq == Seq(1,99,100,4)
  }

  test("equality") {
    forAll { bytes: Array[Byte] =>
      val baBuf = Buf.ByteArray.Owned(bytes)
      val wrappedBB = new ByteBufAsBuf(Unpooled.wrappedBuffer(bytes))
      val wrappedCopiedBB = new ByteBufAsBuf(Unpooled.copiedBuffer(bytes))
      assert(wrappedBB.equals(baBuf))
      assert(wrappedBB.equals(wrappedCopiedBB))
    }
  }

  test("ByteBufAsBuf.slice") {
    val bufSplits = for {
      b <- Arbitrary.arbitrary[Array[Byte]]
      i <- Gen.choose(0, b.length)
      j <- Gen.choose(i, b.length)
      k <- Gen.choose(j, b.length)
    } yield (b, i, j, k)

    forAll(bufSplits) { case (bytes, i, j, k) =>
      val buf = new ByteBufAsBuf(Unpooled.wrappedBuffer(bytes))
      if (i <= j && j <= k) {
        val b1 = buf.slice(i, k)
        val b2 = b1.slice(0, j - i)

        assert(b1.length == k - i)
        assert(b2.length == j - i)
      }
    }
  }
}
