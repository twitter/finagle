package com.twitter.finagle.util

import java.lang.{Double => JDouble, Float => JFloat}
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class BufReaderTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import BufReader._

  def readerWith(bytes: Byte*): BufReader = BufReader(Buf.ByteArray.Owned(bytes.toArray))

  def maskMedium(i: Int) = i & 0x00ffffff

  test("readByte") (forAll { byte: Byte =>
    val br = readerWith(byte)
    assert(br.readByte() == byte)
    val exc = intercept[UnderflowException] { br.readByte() }
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
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readUnsignedMediumBE") (forAll { m: Int =>
    val br = readerWith(
      ((m >> 16) & 0xff).toByte,
      ((m >>  8) & 0xff).toByte,
      ((m      ) & 0xff).toByte
    )
    assert(br.readUnsignedMediumBE() == maskMedium(m))
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readUnsignedMediumLE") (forAll { m: Int =>
    val br = readerWith(
      ((m      ) & 0xff).toByte,
      ((m >>  8) & 0xff).toByte,
      ((m >> 16) & 0xff).toByte
    )
    assert(br.readUnsignedMediumLE() == maskMedium(m))
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readIntBE") (forAll { i: Int =>
    val br = readerWith(
      ((i >> 24) & 0xff).toByte,
      ((i >> 16) & 0xff).toByte,
      ((i >>  8) & 0xff).toByte,
      ((i      ) & 0xff).toByte
    )
    assert(br.readIntBE() == i)
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readIntLE") (forAll { i: Int =>
    val br = readerWith(
      ((i      ) & 0xff).toByte,
      ((i >>  8) & 0xff).toByte,
      ((i >> 16) & 0xff).toByte,
      ((i >> 24) & 0xff).toByte
    )
    assert(br.readIntLE() == i)
    val exc = intercept[UnderflowException] { br.readByte() }
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
    val exc = intercept[UnderflowException] { br.readByte() }
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
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readUnsignedByte") (forAll { b: Byte =>
    val br = new BufReaderImpl(Buf.Empty) { override def readByte() = b }
    assert(br.readUnsignedByte() == (b & 0xff))
  })

  test("readUnsignedShortBE") (forAll { s: Short =>
    val br = new BufReaderImpl(Buf.Empty) { override def readShortBE() = s }
    assert(br.readUnsignedShortBE() == (s & 0xffff))
  })

  test("readUnsignedShortLE") (forAll { s: Short =>
    val br = new BufReaderImpl(Buf.Empty) { override def readShortLE() = s }
    assert(br.readUnsignedShortLE() == (s & 0xffff))
  })

  test("readMediumBE") (forAll { i: Int =>
    val m = maskMedium(i)
    val br = new BufReaderImpl(Buf.Empty) { override def readUnsignedMediumBE() = m }
    val expected = if (m > SignedMediumMax) m | 0xff000000 else m
    assert(br.readMediumBE() == expected)
  })

  test("readMediumLE") (forAll { i: Int =>
    val m = maskMedium(i)
    val br = new BufReaderImpl(Buf.Empty) { override def readUnsignedMediumLE() = m }
    val expected = if (m > SignedMediumMax) m | 0xff000000 else m
    assert(br.readMediumLE() == expected)
  })

  test("readUnsignedIntBE") (forAll { i: Int =>
    val br = new BufReaderImpl(Buf.Empty) { override def readIntBE() = i }
    assert(br.readUnsignedIntBE() == (i & 0xffffffffl))
  })

  test("readUnsignedIntLE") (forAll { i: Int =>
    val br = new BufReaderImpl(Buf.Empty) { override def readIntLE() = i }
    assert(br.readUnsignedIntLE() == (i & 0xffffffffl))
  })

  // .equals is required to handle NaN
  test("readFloatBE") (forAll { i: Int =>
    val br = new BufReaderImpl(Buf.Empty) { override def readIntBE() = i }
    assert(br.readFloatBE().equals(JFloat.intBitsToFloat(i)))
  })

  test("readFloatLE") (forAll { i: Int =>
    val br = new BufReaderImpl(Buf.Empty) { override def readIntLE() = i }
    assert(br.readFloatLE().equals(JFloat.intBitsToFloat(i)))
  })

  test("readDoubleBE") (forAll { l: Long =>
    val br = new BufReaderImpl(Buf.Empty) { override def readLongBE() = l }
    assert(br.readDoubleBE().equals(JDouble.longBitsToDouble(l)))
  })

  test("readDoubleLE") (forAll { l: Long =>
    val br = new BufReaderImpl(Buf.Empty) { override def readLongLE() = l }
    assert(br.readDoubleLE().equals(JDouble.longBitsToDouble(l)))
  })

  test("readBytes") (forAll { bytes: Array[Byte] =>
    val buf = Buf.ByteArray.Owned(bytes ++ bytes)
    val br = BufReader(buf)
    assert(br.readBytes(bytes.length) == Buf.ByteArray.Owned(bytes))
    assert(br.readBytes(bytes.length) == Buf.ByteArray.Owned(bytes))
    assert(br.readBytes(1) == Buf.Empty)
  })

  test("readAll") (forAll { bytes: Array[Byte] =>
    val buf = Buf.ByteArray.Owned(bytes ++ bytes)
    val br = BufReader(buf)
    assert(br.readAll() == Buf.ByteArray.Owned(bytes ++ bytes))
    assert(br.readAll() == Buf.Empty)
  })

  test("underflow if too many bytes are skipped") {
    val br = BufReader(Buf.ByteArray.Owned(new Array[Byte](2)))
    br.skip(2)
    intercept[UnderflowException] {
      br.skip(2)
    }
  }
}
