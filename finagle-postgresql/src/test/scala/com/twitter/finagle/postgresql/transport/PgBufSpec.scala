package com.twitter.finagle.postgresql.transport

import java.nio.ByteBuffer
import java.nio.ByteOrder

import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.Numeric
import com.twitter.finagle.postgresql.Types.NumericSign
import com.twitter.finagle.postgresql.Types.PgArray
import com.twitter.finagle.postgresql.Types.Timestamp
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.scalacheck.Gen
import org.specs2.mutable.Specification

class PgBufSpec extends Specification with PropertiesSpec {

  case class UInt(bits: Int)
  object UInt {
    def apply(l: Long): UInt = UInt((l & 0xffffffff).toInt)
  }
  implicit val arbUInt: Arbitrary[UInt] =
    Arbitrary(Gen.chooseNum(0, Int.MaxValue.toLong * 2).map(UInt(_)))

  "PgBuf" should {

    def expectedBytes[T](value: T, capacity: Int)(expect: (ByteBuffer, T) => ByteBuffer): Array[Byte] = {
      val bb = expect(ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN), value)
      bb.array().slice(bb.arrayOffset(), bb.position())
    }

    def writeFragment[T: Arbitrary](
      name: String,
      capacity: Int = 1024
    )(write: (PgBuf.Writer, T) => PgBuf.Writer)(expect: (ByteBuffer, T) => ByteBuffer) =
      s"write $name" in prop { value: T =>
        val bufWrite = write(PgBuf.writer, value).build
        Buf.ByteArray.Owned.extract(bufWrite) must_== expectedBytes(value, capacity)(expect)
      }

    def readFragment[T: Arbitrary](
      name: String,
      capacity: Int
    )(read: PgBuf.Reader => T)(expect: (ByteBuffer, T) => ByteBuffer) =
      s"read $name" in prop { value: T =>
        read(PgBuf.reader(Buf.ByteArray.Owned(expectedBytes(value, capacity)(expect)))) must_== value
      }

    def fragments[T: Arbitrary](
      name: String,
      capacity: Int = 1024
    )(write: (PgBuf.Writer, T) => PgBuf.Writer)(read: PgBuf.Reader => T)(expect: (ByteBuffer, T) => ByteBuffer) = {
      writeFragment[T](name, capacity)(write)(expect)
      readFragment[T](name, capacity)(read)(expect)

      s"round trip $name" in prop { value: T =>
        read(PgBuf.reader(write(PgBuf.writer, value).build)) must_== value
      }
    }

    fragments[Byte]("byte")(_.byte(_))(_.byte())(_.put(_))
    fragments[Double]("double")(_.double(_))(_.double())(_.putDouble(_))
    fragments[Float]("float")(_.float(_))(_.float())(_.putFloat(_))
    fragments[Int]("int")(_.int(_))(_.int())(_.putInt(_))
    fragments[Long]("long")(_.long(_))(_.long())(_.putLong(_))
    fragments[Numeric]("numeric")(_.numeric(_))(_.numeric()) { (bb, n) =>
      bb.putShort(n.digits.length.toShort)
      bb.putShort(n.weight)
      val sign = n.sign match {
        case NumericSign.Positive => 0
        case NumericSign.Negative => 0x4000
        case NumericSign.NaN => 0xc000
        case NumericSign.Infinity => 0xd000
        case NumericSign.NegInfinity => 0xf000
      }
      bb.putShort(sign.toShort)
      bb.putShort(n.displayScale.toShort)
      n.digits.foreach(d => bb.putShort(d))
      bb
    }
    fragments[UInt]("unsigned int")((w, uint) => w.unsignedInt(uint.bits.toLong))(r => UInt(r.unsignedInt()))(
      (b, uint) => b.putInt(uint.bits)
    )
    // C-style strings only
    fragments[AsciiString]("cstring")((w, str) => w.cstring(str.value))(r => AsciiString(r.cstring())) { (bb, str) =>
      bb.put(str.value.getBytes("UTF8"))
      bb.put(0.toByte)
    }
    fragments[Buf]("buf")(_.buf(_))(_.remainingBuf()) { (bb, buf) =>
      bb.put(Buf.ByteBuffer.Shared.extract(buf))
    }
    fragments[Buf]("framed buf")(_.framedBuf(_))(_.framedBuf()) { (bb, buf) =>
      val value = Buf.ByteArray.Shared.extract(buf)
      bb.putInt(value.length)
      bb.put(value)
    }
    fragments[WireValue]("wire value")(_.value(_))(_.value()) { (bb, value) =>
      value match {
        case WireValue.Null => bb.putInt(-1)
        case WireValue.Value(buf) =>
          val value = Buf.ByteArray.Shared.extract(buf)
          bb.putInt(value.length)
          bb.put(value)
      }
    }
    fragments[Format]("format")(_.format(_))(_.format()) { (bb, format) =>
      format match {
        case Format.Text => bb.putShort(0)
        case Format.Binary => bb.putShort(1)
      }
    }
    fragments[List[Int]]("foreach")(_.foreach(_)(_.int(_)))(_.collect(_.int()).toList) { (bb, xs) =>
      bb.putShort(xs.length.toShort)
      xs.foreach(v => bb.putInt(v))
      bb
    }

    fragments[PgArray]("pgArray", capacity = 65536)(_.array(_))(_.array()) { (bb, arr) =>
      bb.putInt(arr.dimensions)
      bb.putInt(arr.dataOffset)
      bb.putInt(arr.elemType.value.toInt)
      arr.arrayDims.foreach { ad =>
        bb.putInt(ad.size).putInt(ad.lowerBound)
      }
      arr.data.foreach {
        case WireValue.Null => bb.putInt(-1)
        case WireValue.Value(buf) =>
          bb.putInt(buf.length).put(Buf.ByteBuffer.Shared.extract(buf))
      }
      bb
    }
    fragments[Timestamp]("timestamp")(_.timestamp(_))(_.timestamp()) { (bb, ts) =>
      ts match {
        case Timestamp.NegInfinity => bb.putLong(0x8000000000000000L)
        case Timestamp.Infinity => bb.putLong(0x7fffffffffffffffL)
        case Timestamp.Micros(v) => bb.putLong(v)
      }
    }

    "writer" should {
      writeFragment[Buf]("framed")((w, buf) => w.framed(inner => inner.buf(buf).build)) { (bb, buf) =>
        val value = Buf.ByteArray.Shared.extract(buf)
        bb.putInt(value.length + 4)
        bb.put(value)
      }
      writeFragment[Name]("name")(_.name(_)) { (bb, name) =>
        name match {
          case Name.Unnamed => bb.put(Array(0.toByte))
          case Name.Named(name) =>
            bb.put(name.getBytes("UTF-8"))
            bb.put(0.toByte)
        }
      }
      writeFragment[List[Int]]("write foreachUnframed")(_.foreachUnframed(_)(_.int(_))) { (bb, xs) =>
        xs.foreach(v => bb.putInt(v))
        bb
      }

      "opt" in {
        PgBuf.writer.opt[Int](None)(_.int(_)).build.isEmpty must beTrue
        PgBuf.writer.opt[Int](Some(1))(_.int(_)).build.isEmpty must beFalse
      }
    }
  }
}
