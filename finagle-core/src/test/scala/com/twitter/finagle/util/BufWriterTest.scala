package com.twitter.finagle.util

import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class BufWriterTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import BufWriter.OverflowException
  test("writeByte") (forAll { byte: Byte =>
    val bw = BufWriter.fixed(1)
    val buf = bw.writeByte(byte).owned()
    intercept[OverflowException] { bw.writeByte(byte) }
    assert(buf == Buf.ByteArray.Owned(Array(byte)))
  })

  test("writeShortBE") (forAll { short: Short =>
    val bw = BufWriter.fixed(2)
    val buf = bw.writeShortBE(short).owned()
    intercept[OverflowException] { bw.writeByte(0xff) }
    val arr = Array[Byte](
      ((short >> 8) & 0xff).toByte,
      (short & 0xff).toByte
    )
    assert(buf == Buf.ByteArray.Owned(arr))
  })

  test("writeIntBE") (forAll { int: Int =>
    val bw = BufWriter.fixed(4)
    val buf = bw.writeIntBE(int).owned()
    intercept[OverflowException] { bw.writeByte(0xff) }
    val arr = Array[Byte](
      ((int >> 24) & 0xff).toByte,
      ((int >> 16) & 0xff).toByte,
      ((int >> 8) & 0xff).toByte,
      (int & 0xff).toByte
    )
    assert(buf == Buf.ByteArray.Owned(arr))
  })

  test("writeLongBE") (forAll { long: Long =>
    val bw = BufWriter.fixed(8)
    val buf = bw.writeLongBE(long).owned()
    intercept[OverflowException] { bw.writeByte(0xff) }
    val arr = Array[Byte](
      ((long >> 56) & 0xff).toByte,
      ((long >> 48) & 0xff).toByte,
      ((long >> 40) & 0xff).toByte,
      ((long >> 32) & 0xff).toByte,
      ((long >> 24) & 0xff).toByte,
      ((long >> 16) & 0xff).toByte,
      ((long >> 8) & 0xff).toByte,
      (long & 0xff).toByte
    )
    assert(buf == Buf.ByteArray.Owned(arr))
  })

  test("writeBytes") (forAll { bytes: Array[Byte] =>
    val bw = BufWriter.fixed(bytes.length)
    val buf = bw.writeBytes(bytes).owned()
    intercept[OverflowException] { bw.writeByte(0xff) }
    assert(buf == Buf.ByteArray.Owned(bytes))
  })
}