package com.twitter.finagle.util

import com.twitter.io.Buf
import java.math.BigInteger
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class BufReaderTest extends FunSuite with GeneratorDrivenPropertyChecks {
  import BufReader.UnderflowException

  test("readByte") (forAll { byte: Byte =>
    val buf = Buf.ByteArray.Owned(Array(byte))
    val br = BufReader(buf)
    assert(br.readByte() == byte)
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readShortBE") (forAll { short: Short =>
    val buf = Buf.ByteArray.Owned(Array(
      ((short >> 8) & 0xff).toByte,
      (short & 0xff).toByte))
    val br = BufReader(buf)
    // note, we need to cast here toShort so that the
    // MSB is intepreted as the sign bit.
    assert(br.readShortBE().toShort == short)
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readIntBE") (forAll { int: Int =>
    val buf = Buf.ByteArray.Owned(Array(
      ((int >> 24) & 0xff).toByte,
      ((int >> 16) & 0xff).toByte,
      ((int >> 8) & 0xff).toByte,
      (int & 0xff).toByte
    ))
    val br = BufReader(buf)
    assert(br.readIntBE() == int)
    val exc = intercept[UnderflowException] { br.readByte() }
  })

  test("readLongBE") (forAll { long: Long =>
    val buf = Buf.ByteArray.Owned(Array(
      ((long >> 56) & 0xff).toByte,
      ((long >> 48) & 0xff).toByte,
      ((long >> 40) & 0xff).toByte,
      ((long >> 32) & 0xff).toByte,
      ((long >> 24) & 0xff).toByte,
      ((long >> 16) & 0xff).toByte,
      ((long >> 8) & 0xff).toByte,
      (long & 0xff).toByte
    ))
    val br = BufReader(buf)
    assert(br.readLongBE() == long)
    val exc = intercept[UnderflowException] { br.readByte() }
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
}