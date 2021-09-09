package com.twitter.finagle.postgresql.types

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.MalformedInputException
import java.nio.charset.StandardCharsets
import java.time.temporal.ChronoUnit
import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlSpec
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Types.Inet
import com.twitter.finagle.postgresql.Types.NumericSign
import com.twitter.finagle.postgresql.Types.PgArray
import com.twitter.finagle.postgresql.Types.PgArrayDim
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.transport.PgBuf
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.scalacheck.Gen

class ValueReadsSpec extends PgSqlSpec with PropertiesSpec {

  val utf8 = StandardCharsets.UTF_8

  def mkBuf(capacity: Int = 1024)(f: ByteBuffer => ByteBuffer): Buf = {
    val bb = ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN)
    f(bb)
    bb.flip()
    Buf.ByteBuffer.Owned(bb)
  }

  def acceptFragments(reads: ValueReads[_], accept: PgType, accepts: PgType*): Unit = {
    (accept +: accepts).foreach { tpe =>
      s"accept the ${tpe.name} type" in {
        reads.accepts(accept) must be(true)
      }
    }
  }

  def readsFragment[A: Arbitrary, T](
    reads: ValueReads[T],
    accept: PgType,
    f: A => T
  )(
    encode: A => Buf
  ): Unit =
    s"read non-null value" in prop { value: A =>
      val ret = reads.reads(accept, WireValue.Value(encode(value)), utf8)
      ret must be(f(value))
    }

  def arrayReadsFragment[A: Arbitrary, T](
    reads: ValueReads[T],
    accept: PgType,
    f: A => T
  )(
    encode: A => Buf
  ) = {
    val arrayReads = ValueReads.traversableReads[List, T](reads, implicitly)
    s"read one-dimensional array of non-null values" in {
      forAll(Gen.listOfN(5, Arbitrary.arbitrary[A])) { values: List[A] =>
        val data = values.map(v => encode(v)).map(WireValue.Value).toIndexedSeq
        val pgArray = PgArray(
          dimensions = 1,
          dataOffset = 0,
          elemType = accept.oid,
          arrayDims = IndexedSeq(PgArrayDim(values.length, 1)),
          data = data,
        )
        val arrayWire = WireValue.Value(PgBuf.writer.array(pgArray).build)
        val arrayType =
          PgType.arrayOf(accept).getOrElse(sys.error(s"no array type for ${accept.name}"))
        val ret = arrayReads.reads(arrayType, arrayWire, utf8)
        ret must be(values.map(f))
      } // limit to 5 elements to speed things up
    }
  }

  def nonNullableFragment(reads: ValueReads[_], accept: PgType): Unit =
    s"fail to read a null value" in {
      a[IllegalArgumentException] must be thrownBy reads.reads(accept, WireValue.Null, utf8)
    }

  def nullableFragment(reads: ValueReads[_], accept: PgType): Unit =
    "is nullable when wrapped in Option" in {
      ValueReads
        .optionReads(reads).reads(accept, WireValue.Null, utf8) must be(None)
    }

  def specs[A: Arbitrary, T](
    reads: ValueReads[T],
    accept: PgType,
    accepts: PgType*
  )(
    f: A => T
  )(
    encode: A => Buf
  ): Unit = {
    acceptFragments(reads, accept, accepts: _*)
    readsFragment(reads, accept, f)(encode)
    arrayReadsFragment(reads, accept, f)(encode)
    nonNullableFragment(reads, accept)
    nullableFragment(reads, accept)
  }

  def simpleSpec[T: Arbitrary](
    reads: ValueReads[T],
    accept: PgType,
    accepts: PgType*
  )(
    encode: T => Buf
  ): Unit =
    specs[T, T](reads, accept, accepts: _*)(identity)(encode)

  "ValueReads" should {
    "simple" should {
      "fail when the value buffer is not consumed entirely" in {
        val invalid = ValueReads.simple(PgType.Int4)(_.short())
        the[PgSqlClientError] thrownBy
          invalid.reads(
            PgType.Int4,
            Buf.ByteArray(0, 0, 0, 0),
            utf8) must have message "Reading value of type int4 should have consumed the whole value's buffer, but 2 bytes remained."
      }
      "fail when the value is null" in {
        val valid = ValueReads.simple(PgType.Int4)(_.int())
        the[IllegalArgumentException] thrownBy valid.reads(
          PgType.Int4,
          WireValue.Null,
          utf8) must have message "Type int4 has no reasonable null value. If you intended to make this field nullable, you must read it as an Option[T]."
      }
    }

    "by" should {
      "accept the underlying type" in {
        val longByInt = ValueReads.by[Int, Long](_.toLong)
        longByInt.accepts(PgType.Int4) must be(true)
        longByInt.accepts(PgType.Int8) must be(false)
      }
      "reads the underlying value" in prop { value: Int =>
        val longByInt = ValueReads.by[Int, Long](_.toLong)
        longByInt.reads(PgType.Int4, Buf.U32BE(value), utf8) must be(value.toLong)
      }
    }

    "or" should {
      "accept both types" in {
        val first = ValueReads.simple(PgType.Int4)(_ => 4)
        val second = ValueReads.simple(PgType.Int2)(_ => 2)

        val or = ValueReads.or(first, second)
        or.accepts(PgType.Int4) must be(true)
        or.accepts(PgType.Int2) must be(true)
        or.accepts(PgType.Int8) must be(false)

        val orElse = first orElse second
        orElse.accepts(PgType.Int4) must be(true)
        orElse.accepts(PgType.Int2) must be(true)
        orElse.accepts(PgType.Int8) must be(false)
      }
      "reads from both" in {
        val first = ValueReads.simple(PgType.Int4)(_ => 4)
        val second = ValueReads.simple(PgType.Int2)(_ => 2)

        val or = ValueReads.or(first, second)
        or.reads(PgType.Int4, Buf.Empty, utf8) must be(4)
        or.reads(PgType.Int2, Buf.Empty, utf8) must be(2)

        val orElse = first orElse second
        orElse.reads(PgType.Int4, Buf.Empty, utf8) must be(4)
        orElse.reads(PgType.Int2, Buf.Empty, utf8) must be(2)
      }
      "reads from first in priority" in {
        val first = ValueReads.simple(PgType.Int4)(_ => 4)
        val second = ValueReads.simple(PgType.Int4)(_ => 2)

        val or = ValueReads.or(first, second)
        or.reads(PgType.Int4, Buf.Empty, utf8) must be(4)

        val orElse = first orElse second
        orElse.reads(PgType.Int4, Buf.Empty, utf8) must be(4)
      }
    }

    "optionReads" should {
      "delegate reads when non-null" in {
        val optionalInt = ValueReads.optionReads(ValueReads.readsInt)
        val read = optionalInt.reads(PgType.Int4, WireValue.Value(Buf.ByteArray(0, 0, 0, 0)), utf8)
        read must be(Some(0))
      }
      "accept the underlying type" in {
        val optionalInt = ValueReads.optionReads(ValueReads.readsInt)
        optionalInt.accepts(PgType.Int4) must be(true)
        optionalInt.accepts(PgType.Text) must be(false)
      }
      "return None when null" in {
        val optionalInt = ValueReads.optionReads(ValueReads.readsInt)
        val read = optionalInt.reads(PgType.Int4, WireValue.Null, utf8)
        read must be(None)
      }
    }

    "traversableReads" should {
      "accept the underlying type" in {
        val readsIntList = ValueReads.traversableReads[List, Int](ValueReads.readsInt, implicitly)
        readsIntList.accepts(PgType.Int4Array) must be(true)
        readsIntList.accepts(PgType.Int4) must be(false)
        readsIntList.accepts(PgType.Int8Array) must be(false)
      }

      "reject nona=-array types when reading" in {
        val readsIntList = ValueReads.traversableReads[List, Int](ValueReads.readsInt, implicitly)
        the[PgSqlClientError] thrownBy readsIntList.reads(
          PgType.Int4,
          Buf.Empty,
          utf8) must have message s"Type int4 is not an array type and cannot be read as such."
      }

      "support empty lists" in {
        val readsIntList = ValueReads.traversableReads[List, Int](ValueReads.readsInt, implicitly)
        val pgArray = PgArray(
          dimensions = 0,
          dataOffset = 0,
          elemType = PgType.Int4.oid,
          arrayDims = IndexedSeq.empty,
          data = IndexedSeq.empty
        )
        val arrayBuf = PgBuf.writer.array(pgArray).build
        val read = readsIntList.reads(PgType.Int4Array, arrayBuf, utf8)
        read must be(Nil)
      }

      "fail for multi-dimensional arrays" in {
        val readsIntList = ValueReads.traversableReads[List, Int](ValueReads.readsInt, implicitly)

        val pgArray = PgArray(
          dimensions = 2,
          dataOffset = 0,
          elemType = PgType.Int4.oid,
          arrayDims = IndexedSeq(PgArrayDim(0, 1), PgArrayDim(0, 1)),
          data = IndexedSeq.empty
        )
        val arrayBuf = PgBuf.writer.array(pgArray).build

        the[PgSqlClientError] thrownBy readsIntList.reads(
          PgType.Int4Array,
          arrayBuf,
          utf8) must have message "Multi dimensional arrays are not supported. Expected 0 or 1 dimensions, got 2"
      }
    }

    "readsBigDecimal" should simpleSpec[BigDecimal](ValueReads.readsBigDecimal, PgType.Numeric) {
      bd =>
        mkBuf() { bb =>
          // converting to numeric is non-trivial, so we don't re-write it here.
          val numeric = PgNumeric.bigDecimalToNumeric(bd)
          bb.putShort(numeric.digits.length.toShort)
          bb.putShort(numeric.weight)
          numeric.sign match {
            case NumericSign.Positive => bb.putShort(0)
            case NumericSign.Negative => bb.putShort(0x4000)
            case _ => sys.error("unexpected sign")
          }
          bb.putShort(numeric.displayScale.toShort)
          numeric.digits.foreach(bb.putShort)
          bb
        }
    }
    "readsBoolean" should simpleSpec[Boolean](ValueReads.readsBoolean, PgType.Bool) {
      case true => Buf.ByteArray(0x01)
      case false => Buf.ByteArray(0x00)
    }
    "readsBuf" should simpleSpec[Buf](ValueReads.readsBuf, PgType.Bytea) { buf =>
      mkBuf() { bb =>
        bb.put(Buf.ByteArray.Shared.extract(buf))
      }
    }
    "readsByte" should simpleSpec[Byte](ValueReads.readsByte, PgType.Int2) { byte =>
      mkBuf() { bb =>
        bb.putShort(byte.toShort)
      }
    }
    "readsByte" should {
      def failFor(value: Short) = {
        val buf = mkBuf()(bb => bb.putShort(value))
        the[PgSqlClientError] thrownBy ValueReads.readsByte.reads(
          PgType.Int2,
          WireValue.Value(buf),
          utf8) must have message
          s"int2 value is out of range for reading as a Byte: $value is not within [-128,127]. Consider reading as Short instead."
      }
      "fail when int2 value is out of range" in {
        failFor((Byte.MaxValue.toInt + 1).toShort)
        failFor((Byte.MinValue.toInt - 1).toShort)
      }
    }
    "readsDouble" should {
      "read float4" should specs[Float, Double](ValueReads.readsDouble, PgType.Float4)(_.toDouble) {
        float =>
          mkBuf()(_.putFloat(float))
      }
      "read float8" should simpleSpec[Double](ValueReads.readsDouble, PgType.Float8) { double =>
        mkBuf()(_.putDouble(double))
      }
    }
    "readsFloat8" should simpleSpec[Double](ValueReads.readsFloat8, PgType.Float8) { double =>
      mkBuf()(_.putDouble(double))
    }
    "readsFloat" should simpleSpec[Float](ValueReads.readsFloat, PgType.Float4) { float =>
      mkBuf()(_.putFloat(float))
    }
    "readsInet" should simpleSpec[Inet](ValueReads.readsInet, PgType.Inet) { inet =>
      mkBuf() { bb =>
        inet.ipAddress match {
          case _: java.net.Inet4Address => bb.put(2.toByte)
          case _: java.net.Inet6Address => bb.put(3.toByte)
        }
        bb.put(inet.netmask.toByte)
          .put(0.toByte)
        val addr = inet.ipAddress.getAddress
        bb.put(addr.length.toByte)
        bb.put(addr)
      }
    }
    "readsInstant" should simpleSpec[java.time.Instant](
      ValueReads.readsInstant,
      PgType.Timestamptz,
      PgType.Timestamp) { ts =>
      mkBuf() { bb =>
        val sincePgEpoch = java.time.Duration.between(PgTime.Epoch, ts)
        val secs = sincePgEpoch.getSeconds
        val nanos = sincePgEpoch.getNano
        val micros = secs * 1000000 + nanos / 1000
        bb.putLong(micros)
      }
    }
    "readsInstant" should {
      def failFor(name: String, value: Long) =
        s"fail for $name" in {
          val buf = mkBuf() { bb =>
            bb.putLong(value)
          }
          a[PgSqlClientError] must be thrownBy ValueReads.readsInstant.reads(
            PgType.Timestamptz,
            WireValue.Value(buf),
            utf8)
        }
      failFor("-Infinity", 0x8000000000000000L)
      failFor("Infinity", 0x7fffffffffffffffL)
    }
    "readsInt4" should simpleSpec[Int](ValueReads.readsInt4, PgType.Int4) { int =>
      mkBuf()(_.putInt(int))
    }
    "readsInt8" should simpleSpec[Long](ValueReads.readsInt8, PgType.Int8) { long =>
      mkBuf()(_.putLong(long))
    }
    "readsInt" should {
      "read int2" should specs[Short, Int](ValueReads.readsInt, PgType.Int2)(_.toInt) { short =>
        mkBuf()(_.putShort(short))
      }
      "read int4" should simpleSpec[Int](ValueReads.readsInt, PgType.Int4) { int =>
        mkBuf()(_.putInt(int))
      }
    }
    "readsJson" should simpleSpec[Json](ValueReads.readsJson, PgType.Json) { json =>
      mkBuf(json.jsonByteArray.length) { bb =>
        bb.put(json.jsonByteBuffer)
      }
    }
    "readsJsonb" should simpleSpec[Json](ValueReads.readsJson, PgType.Jsonb) { json =>
      mkBuf(json.jsonByteArray.length + 1) { bb =>
        bb.put(1.toByte).put(json.jsonByteBuffer)
      }
    }
    "readsLocalDate" should simpleSpec[java.time.LocalDate](
      ValueReads.readsLocalDate,
      PgType.Date) { ld =>
      mkBuf() { bb =>
        bb.putInt(ChronoUnit.DAYS.between(PgDate.Epoch, ld).toInt)
      }
    }
    "readsLong" should {
      "read int2" should specs[Short, Long](ValueReads.readsLong, PgType.Int2)(_.toLong) { short =>
        mkBuf()(_.putShort(short))
      }
      "read int4" should specs[Int, Long](ValueReads.readsLong, PgType.Int4)(_.toLong) { int =>
        mkBuf()(_.putInt(int))
      }
      "read int8" should simpleSpec[Long](ValueReads.readsLong, PgType.Int8) { long =>
        mkBuf()(_.putLong(long))
      }
    }
    "readsShort" should simpleSpec[Short](ValueReads.readsShort, PgType.Int2) { short =>
      mkBuf() { bb =>
        bb.putShort(short)
      }
    }
    "readsString" should simpleSpec[String](
      ValueReads.readsString,
      PgType.Text,
      PgType.Json,
      PgType.Varchar,
      PgType.Bpchar,
      PgType.Name,
      PgType.Unknown
    ) { string =>
      mkBuf(string.getBytes(utf8).length) { bb =>
        bb.put(string.getBytes(utf8))
      }
    }
    "readsString" should {
      "fail for malformed utf8" in {
        a[MalformedInputException] shouldBe thrownBy(
          ValueReads.readsString.reads(PgType.Text, Buf.ByteArray(0xc3.toByte, 0x28.toByte), utf8))
      }
    }
    "readsUuid" should simpleSpec[java.util.UUID](ValueReads.readsUuid, PgType.Uuid) { uuid =>
      mkBuf() { bb =>
        bb.putLong(uuid.getMostSignificantBits)
        bb.putLong(uuid.getLeastSignificantBits)
      }
    }
  }
}
