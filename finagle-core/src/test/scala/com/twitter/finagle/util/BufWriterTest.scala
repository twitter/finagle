package com.twitter.finagle.util

import com.twitter.io.Buf
import java.lang.{Double => JDouble, Float => JFloat}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class BufWriterTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import BufWriter.OverflowException

  test("index initialized to zero") {
    assert(BufWriter.fixed(1).index == 0)
  }

  test("writeByte") (forAll { byte: Byte =>
    val bw = BufWriter.fixed(1)
    val buf = bw.writeByte(byte).owned()
    intercept[OverflowException] { bw.writeByte(byte) }
    assert(buf == Buf.ByteArray.Owned(Array(byte)))
  })

  test("writeShort{BE,LE}") (forAll { s: Short =>
    val be = BufWriter.fixed(2).writeShortBE(s)
    val le = BufWriter.fixed(2).writeShortLE(s)

    intercept[OverflowException] { be.writeByte(0xff) }
    intercept[OverflowException] { be.writeByte(0xff) }

    val arr = Array[Byte](
      ((s >>  8) & 0xff).toByte,
      ((s      ) & 0xff).toByte
    )

    assert(be.owned() == Buf.ByteArray.Owned(arr))
    assert(le.owned() == Buf.ByteArray.Owned(arr.reverse))
    assert(be.index == 2)
    assert(le.index == 2)
  })

  test("writeMedium{BE,LE}") (forAll { m: Int =>
    val be = BufWriter.fixed(3).writeMediumBE(m)
    val le = BufWriter.fixed(3).writeMediumLE(m)

    intercept[OverflowException] { be.writeByte(0xff) }
    intercept[OverflowException] { be.writeByte(0xff) }

    val arr = Array[Byte](
      ((m >> 16) & 0xff).toByte,
      ((m >>  8) & 0xff).toByte,
      ((m      ) & 0xff).toByte
    )

    assert(be.owned() == Buf.ByteArray.Owned(arr))
    assert(le.owned() == Buf.ByteArray.Owned(arr.reverse))
    assert(be.index == 3)
    assert(le.index == 3)
  })

  test("writeInt{BE,LE}") (forAll { i: Int =>
    val be = BufWriter.fixed(4).writeIntBE(i)
    val le = BufWriter.fixed(4).writeIntLE(i)

    intercept[OverflowException] { be.writeByte(0xff) }
    intercept[OverflowException] { be.writeByte(0xff) }

    val arr = Array[Byte](
      ((i >> 24) & 0xff).toByte,
      ((i >> 16) & 0xff).toByte,
      ((i >>  8) & 0xff).toByte,
      ((i      ) & 0xff).toByte
    )

    assert(be.owned() == Buf.ByteArray.Owned(arr))
    assert(le.owned() == Buf.ByteArray.Owned(arr.reverse))
    assert(be.index == 4)
    assert(le.index == 4)
  })

  test("writeLong{BE,LE}") (forAll { l: Long =>
    val be = BufWriter.fixed(8).writeLongBE(l)
    val le = BufWriter.fixed(8).writeLongLE(l)

    intercept[OverflowException] { be.writeByte(0xff) }
    intercept[OverflowException] { be.writeByte(0xff) }

    val arr = Array[Byte](
      ((l >> 56) & 0xff).toByte,
      ((l >> 48) & 0xff).toByte,
      ((l >> 40) & 0xff).toByte,
      ((l >> 32) & 0xff).toByte,
      ((l >> 24) & 0xff).toByte,
      ((l >> 16) & 0xff).toByte,
      ((l >>  8) & 0xff).toByte,
      ((l      ) & 0xff).toByte
    )

    assert(be.owned() == Buf.ByteArray.Owned(arr))
    assert(le.owned() == Buf.ByteArray.Owned(arr.reverse))
    assert(be.index == 8)
    assert(le.index == 8)
  })

  test("writeFloat{BE,LE}") (forAll { f: Float =>
    val be = BufWriter.fixed(4).writeFloatBE(f)
    val le = BufWriter.fixed(4).writeFloatLE(f)

    intercept[OverflowException] { be.writeByte(0xff) }
    intercept[OverflowException] { be.writeByte(0xff) }

    val i = JFloat.floatToIntBits(f)

    val arr = Array[Byte](
      ((i >> 24) & 0xff).toByte,
      ((i >> 16) & 0xff).toByte,
      ((i >>  8) & 0xff).toByte,
      ((i      ) & 0xff).toByte
    )

    assert(be.owned() == Buf.ByteArray.Owned(arr))
    assert(le.owned() == Buf.ByteArray.Owned(arr.reverse))
    assert(be.index == 4)
    assert(le.index == 4)
  })

  test("writeDouble{BE,LE}") (forAll { d: Double =>
    val be = BufWriter.fixed(8).writeDoubleBE(d)
    val le = BufWriter.fixed(8).writeDoubleLE(d)

    intercept[OverflowException] { be.writeByte(0xff) }
    intercept[OverflowException] { be.writeByte(0xff) }

    val l = JDouble.doubleToLongBits(d)

    val arr = Array[Byte](
      ((l >> 56) & 0xff).toByte,
      ((l >> 48) & 0xff).toByte,
      ((l >> 40) & 0xff).toByte,
      ((l >> 32) & 0xff).toByte,
      ((l >> 24) & 0xff).toByte,
      ((l >> 16) & 0xff).toByte,
      ((l >>  8) & 0xff).toByte,
      ((l      ) & 0xff).toByte
    )

    assert(be.owned() == Buf.ByteArray.Owned(arr))
    assert(le.owned() == Buf.ByteArray.Owned(arr.reverse))
    assert(be.index == 8)
    assert(le.index == 8)
  })

  test("writeBytes") (forAll { bytes: Array[Byte] =>
    val bw = BufWriter.fixed(bytes.length)
    val buf = bw.writeBytes(bytes).owned()
    intercept[OverflowException] { bw.writeByte(0xff) }
    assert(buf == Buf.ByteArray.Owned(bytes))
    assert(bw.index == bytes.length)
  })
}
