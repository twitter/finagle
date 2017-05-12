package com.twitter.finagle.netty4

import com.twitter.io.ByteReader.UnderflowException
import com.twitter.io.{Buf, ByteReader}
import io.netty.buffer.{ByteBuf, UnpooledByteBufAllocator}
import java.lang.{Double => JDouble, Float => JFloat}
import org.scalatest.FunSuite
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class CopyingByteBufByteReaderTest extends AbstractByteBufByteReaderTest {
  override protected def wrapByteBufInReader(bb: ByteBuf): ByteReader =
    new CopyingByteBufByteReader(bb)

  test("Buf instances are backed by a precisely sized Buf.ByteArray") {
    val br = readerWith(0x00, 0x01)
    br.readBytes(1) match {
      case Buf.ByteArray.Owned(data, begin, end) =>
        assert(data.sameElements(Seq(0x00)))
        assert(begin == 0)
        assert(end == 1)

      case other =>
        val name = other.getClass.getSimpleName
        fail(s"Unexpected representation of returned `Buf` instance: $name")
    }
  }
}

abstract class AbstractByteBufByteReaderTest extends FunSuite with GeneratorDrivenPropertyChecks {

  private val SignedMediumMax = 0x800000

  protected def wrapByteBufInReader(bb: ByteBuf): ByteReader

  protected def newReader(f: ByteBuf => Unit): ByteReader = {
    val buf = UnpooledByteBufAllocator.DEFAULT.buffer(10, Int.MaxValue)
    f(buf)
    wrapByteBufInReader(buf)
  }

  protected def readerWith(bytes: Byte*): ByteReader = newReader { bb =>
    bytes.foreach(bb.writeByte(_))
  }

  private def maskMedium(i: Int) = i & 0x00ffffff

  test("readByte") (forAll { byte: Byte =>
    val br = readerWith(byte)
    assert(br.readByte() == byte)
    intercept[UnderflowException] { br.readByte() }
  })

  test("readShortBE") (forAll { s: Short =>
    val br = readerWith(
      ((s >>  8) & 0xff).toByte,
      ((s      ) & 0xff).toByte
    )
    // note, we need to cast here toShort so that the
    // MSB is interpreted as the sign bit.
    assert(br.readShortBE() == s)
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readShortLE") (forAll { s: Short =>
    val br = readerWith(
      ((s      ) & 0xff).toByte,
      ((s >>  8) & 0xff).toByte
    )

    // note, we need to cast here toShort so that the
    // MSB is interpreted as the sign bit.
    assert(br.readShortLE() == s)
    intercept[UnderflowException] { br.readByte() }
  })

  test("readUnsignedMediumBE") (forAll { m: Int =>
    val br = readerWith(
      ((m >> 16) & 0xff).toByte,
      ((m >>  8) & 0xff).toByte,
      ((m      ) & 0xff).toByte
    )
    assert(br.readUnsignedMediumBE() == maskMedium(m))
    intercept[UnderflowException] { br.readByte() }
  })

  test("readUnsignedMediumLE") (forAll { m: Int =>
    val br = readerWith(
      ((m      ) & 0xff).toByte,
      ((m >>  8) & 0xff).toByte,
      ((m >> 16) & 0xff).toByte
    )
    assert(br.readUnsignedMediumLE() == maskMedium(m))
    intercept[UnderflowException] { br.readByte() }
  })

  test("readIntBE") (forAll { i: Int =>
    val br = readerWith(
      ((i >> 24) & 0xff).toByte,
      ((i >> 16) & 0xff).toByte,
      ((i >>  8) & 0xff).toByte,
      ((i      ) & 0xff).toByte
    )
    assert(br.readIntBE() == i)
    intercept[UnderflowException] { br.readByte() }
  })

  test("readIntLE") (forAll { i: Int =>
    val br = readerWith(
      ((i      ) & 0xff).toByte,
      ((i >>  8) & 0xff).toByte,
      ((i >> 16) & 0xff).toByte,
      ((i >> 24) & 0xff).toByte
    )
    assert(br.readIntLE() == i)
    intercept[UnderflowException] { br.readByte() }
  })

  test("readLongBE") (forAll { l: Long =>
    val br = readerWith(
      ((l >> 56) & 0xff).toByte,
      ((l >> 48) & 0xff).toByte,
      ((l >> 40) & 0xff).toByte,
      ((l >> 32) & 0xff).toByte,
      ((l >> 24) & 0xff).toByte,
      ((l >> 16) & 0xff).toByte,
      ((l >>  8) & 0xff).toByte,
      ((l      ) & 0xff).toByte
    )
    assert(br.readLongBE() == l)
    intercept[UnderflowException] { br.readByte() }
  })

  test("readLongLE") (forAll { l: Long =>
    val br = readerWith(
      ((l      ) & 0xff).toByte,
      ((l >>  8) & 0xff).toByte,
      ((l >> 16) & 0xff).toByte,
      ((l >> 24) & 0xff).toByte,
      ((l >> 32) & 0xff).toByte,
      ((l >> 40) & 0xff).toByte,
      ((l >> 48) & 0xff).toByte,
      ((l >> 56) & 0xff).toByte
    )
    assert(br.readLongLE() == l)
    intercept[UnderflowException] { br.readByte() }
  })

  test("readUnsignedByte") (forAll { b: Byte =>
    val br = newReader(_.writeByte(b))
    assert(br.readUnsignedByte() == (b & 0xff))
  })

  test("readUnsignedShortBE") (forAll { s: Short =>
    val br = newReader(_.writeShort(s))
    assert(br.readUnsignedShortBE() == (s & 0xffff))
  })

  test("readUnsignedShortLE") (forAll { s: Short =>
    val br = newReader(_.writeShortLE(s))
    assert(br.readUnsignedShortLE() == (s & 0xffff))
  })

  test("readMediumBE") (forAll { i: Int =>
    val m = maskMedium(i)
    val br = newReader(_.writeMedium(m))
    val expected = if (m > SignedMediumMax) m | 0xff000000 else m
    assert(br.readMediumBE() == expected)
  })

  test("readMediumLE") (forAll { i: Int =>
    val m = maskMedium(i)
    val br = newReader(_.writeMediumLE(m))
    val expected = if (m > SignedMediumMax) m | 0xff000000 else m
    assert(br.readMediumLE() == expected)
  })

  test("readUnsignedIntBE") (forAll { i: Int =>
    val br = newReader(_.writeInt(i))
    assert(br.readUnsignedIntBE() == (i & 0xffffffffl))
  })

  test("readUnsignedIntLE") (forAll { i: Int =>
    val br = newReader(_.writeIntLE(i))
    assert(br.readUnsignedIntLE() == (i & 0xffffffffl))
  })

  // .equals is required to handle NaN
  test("readFloatBE") (forAll { i: Int =>
    val br = newReader(_.writeInt(i))
    assert(br.readFloatBE().equals(JFloat.intBitsToFloat(i)))
  })

  test("readFloatLE") (forAll { i: Int =>
    val br = newReader(_.writeIntLE(i))
    assert(br.readFloatLE().equals(JFloat.intBitsToFloat(i)))
  })

  test("readDoubleBE") (forAll { l: Long =>
    val br = newReader(_.writeLong(l))
    assert(br.readDoubleBE().equals(JDouble.longBitsToDouble(l)))
  })

  test("readDoubleLE") (forAll { l: Long =>
    val br = newReader(_.writeLongLE(l))
    assert(br.readDoubleLE().equals(JDouble.longBitsToDouble(l)))
  })

  test("readBytes") (forAll { bytes: Array[Byte] =>
    val br = readerWith(bytes ++ bytes:_*)
    intercept[IllegalArgumentException] { br.readBytes(-1) }
    assert(br.readBytes(bytes.length) == Buf.ByteArray.Owned(bytes))
    assert(br.readBytes(bytes.length) == Buf.ByteArray.Owned(bytes))
    assert(br.readBytes(1) == Buf.Empty)
  })

  test("readAll") (forAll { bytes: Array[Byte] =>
    val br = readerWith(bytes ++ bytes:_*)
    assert(br.readAll() == Buf.ByteArray.Owned(bytes ++ bytes))
    assert(br.readAll() == Buf.Empty)
  })

  test("underflow if too many bytes are skipped") {
    val br = readerWith(0x0, 0x0)
    br.skip(2)
    intercept[UnderflowException] {
      br.skip(2)
    }
  }

  test("remainingUntil") {
    forAll { (bytes: Array[Byte], byte: Byte) =>
      val buf = Buf.ByteArray.Owned(bytes ++ Array(byte) ++ bytes)
      val br = readerWith(bytes ++ Array(byte) ++ bytes:_*)

      val remainingBefore = br.remaining
      val until = br.remainingUntil(byte)
      assert(remainingBefore == br.remaining)
      val before = br.readBytes(until)
      val pivot = br.readByte()
      val after = br.readAll()

      val expected = before.concat(Buf.ByteArray.Owned(Array(pivot))).concat(after)
      assert(pivot == byte && expected == buf)
    }

    assert(readerWith().remainingUntil(0x0) == -1)

    val reader = readerWith(0x1, 0x2, 0x3)
    assert(0 == reader.remainingUntil(0x1))
    assert(1 == reader.remainingUntil(0x2))
    assert(2 == reader.remainingUntil(0x3))

    assert(0x1 == reader.readByte())
    assert(2 == reader.remaining)
    assert(-1 == reader.remainingUntil(0x1))
    assert(0 == reader.remainingUntil(0x2))
    assert(1 == reader.remainingUntil(0x3))
    assert(-1 == reader.remainingUntil(0x4))
    assert(2 == reader.remaining)

    assert(0x2 == reader.readByte())
    assert(0x3 == reader.readByte())
    assert(0 == reader.remaining)
    assert(-1 == reader.remainingUntil(0x3))
  }

  test("close() releases the underlying ByteBuf and is idempotent") {
    val byteBuf = UnpooledByteBufAllocator.DEFAULT.buffer(10, Int.MaxValue)
    val br = wrapByteBufInReader(byteBuf)
    assert(byteBuf.refCnt() == 1)
    br.close()
    assert(byteBuf.refCnt() == 0)

    // idempotency
    br.close() // would throw if not guarded by the ByteReader implementation
    assert(byteBuf.refCnt() == 0)
  }
}
