package com.twitter.finagle.postgresql.types

import java.nio.ByteBuffer
import java.nio.ByteOrder
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

class ValueWritesSpec extends PgSqlSpec with PropertiesSpec {

  val utf8 = StandardCharsets.UTF_8

  def mkBuf(capacity: Int = 1024)(f: ByteBuffer => ByteBuffer): Buf = {
    val bb = ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN)
    f(bb)
    bb.flip()
    Buf.ByteBuffer.Owned(bb)
  }

  def acceptFragments(writes: ValueWrites[_], accept: PgType, accepts: PgType*): Unit = {
    (accept +: accepts).foreach { tpe =>
      s"accept the ${tpe.name} type" in {
        writes.accepts(accept) must be(true)
      }
    }
  }

  def writesFragment[T: Arbitrary](writes: ValueWrites[T], accept: PgType)(encode: T => Buf): Unit =
    s"write non-null value" in { value: T =>
      val ret = writes.writes(accept, value, utf8)
      ret must be(WireValue.Value(encode(value)))
    }

  def arrayWritesFragment[T: Arbitrary](
    writes: ValueWrites[T],
    accept: PgType
  )(
    encode: T => Buf
  ) = {
    val arrayWrites = ValueWrites.traversableWrites[List, T](writes)
    s"write one-dimensional array of non-null values" in {
      forAll(Gen.listOfN(5, Arbitrary.arbitrary[T])) { values: List[T] =>
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
        val ret = arrayWrites.writes(arrayType, values, utf8)
        ret must be(arrayWire)
      }
    } // limit to 5 elements to speed things up
  }

  def nullableFragment[T: Arbitrary](writes: ValueWrites[T], accept: PgType): Unit =
    "is nullable when wrapped in Option" in {
      ValueWrites.optionWrites(writes).writes(accept, None, utf8) must be(WireValue.Null)
    }

  def simpleSpec[T: Arbitrary](
    writes: ValueWrites[T],
    accept: PgType,
    accepts: PgType*
  )(
    encode: T => Buf
  ): Unit = {
    acceptFragments(writes, accept, accepts: _*)
    writesFragment(writes, accept)(encode)
    arrayWritesFragment(writes, accept)(encode)
    nullableFragment(writes, accept)
  }

  "ValueWrites" should {

    "by" should {
      "accept the underlying type" in {
        val intByLong = ValueWrites.by[Long, Int](_.toLong)
        intByLong.accepts(PgType.Int4) must be(false)
        intByLong.accepts(PgType.Int8) must be(true)
      }
      "write the underlying value" in { value: Int =>
        val intByLong = ValueWrites.by[Long, Int](_.toLong)
        val wrote = intByLong.writes(PgType.Int8, value, utf8)
        wrote must be(WireValue.Value(Buf.U64BE(value.toLong)))
      }
    }

    "or" should {
      val first = ValueWrites.simple[Int](PgType.Int4) { case (w, _) => w.byte(4) }
      val second = ValueWrites.simple[Int](PgType.Int2) { case (w, _) => w.byte(2) }
      val firstValue = WireValue.Value(Buf.ByteArray(4))
      val secondValue = WireValue.Value(Buf.ByteArray(2))

      "accept both types" in {
        val or = ValueWrites.or(first, second)
        or.accepts(PgType.Int4) must be(true)
        or.accepts(PgType.Int2) must be(true)
        or.accepts(PgType.Int8) must be(false)

        val orElse = first orElse second
        orElse.accepts(PgType.Int4) must be(true)
        orElse.accepts(PgType.Int2) must be(true)
        orElse.accepts(PgType.Int8) must be(false)
      }
      "writes to both" in {
        val or = ValueWrites.or(first, second)
        or.writes(PgType.Int4, 4, utf8) must be(firstValue)
        or.writes(PgType.Int2, 4, utf8) must be(secondValue)

        val orElse = first orElse second
        orElse.writes(PgType.Int4, 4, utf8) must be(firstValue)
        orElse.writes(PgType.Int2, 4, utf8) must be(secondValue)
      }
      "writes to first in priority" in {
        val second = ValueWrites.simple[Int](PgType.Int4) { case (w, _) => w.byte(2) }

        val or = ValueWrites.or(first, second)
        or.writes(PgType.Int4, 4, utf8) must be(firstValue)

        val orElse = first orElse second
        orElse.writes(PgType.Int4, 4, utf8) must be(firstValue)
      }
    }

    "optionWrites" should {
      "delegate writes when Some" in {
        val optionalInt = ValueWrites.optionWrites(ValueWrites.writesInt)
        val wrote = optionalInt.writes(PgType.Int4, Some(0), utf8)
        wrote must be(ValueWrites.writesInt.writes(PgType.Int4, 0, utf8))
      }
      "accept the underlying type" in {
        val optionalInt = ValueWrites.optionWrites(ValueWrites.writesInt)
        optionalInt.accepts(PgType.Int4) must be(true)
        optionalInt.accepts(PgType.Text) must be(false)
      }
      "write Null when None" in {
        val optionalInt = ValueWrites.optionWrites(ValueWrites.writesInt)
        val wrote = optionalInt.writes(PgType.Int4, None, utf8)
        wrote must be(WireValue.Null)
      }
    }

    "traversableWrites" should {
      "accept the underlying type" in {
        val writesIntList = ValueWrites.traversableWrites[List, Int](ValueWrites.writesInt)
        writesIntList.accepts(PgType.Int4Array) must be(true)
        writesIntList.accepts(PgType.Int4) must be(false)
        writesIntList.accepts(PgType.Int2Array) must be(false)
      }

      "reject non-array types when reading" in {
        val writesIntList = ValueWrites.traversableWrites[List, Int](ValueWrites.writesInt)
        the[PgSqlClientError] thrownBy writesIntList.writes(
          PgType.Int4,
          Nil,
          utf8) must have message
          "Type int4 is not an array type and cannot be written as such. Note that this may be because you're trying to write a multi-dimensional array which isn't supported."
      }

      "support empty lists" in {
        val writesIntList = ValueWrites.traversableWrites[List, Int](ValueWrites.writesInt)
        val pgArray = PgArray(
          dimensions = 0,
          dataOffset = 0,
          elemType = PgType.Int4.oid,
          arrayDims = IndexedSeq.empty,
          data = IndexedSeq.empty
        )
        val arrayBuf = PgBuf.writer.array(pgArray).build
        writesIntList.writes(PgType.Int4Array, Nil, utf8) must be(WireValue.Value(arrayBuf))
      }

      "fail for multi-dimensional arrays" in {
        val writesIntList = ValueWrites.traversableWrites[List, Int](ValueWrites.writesInt)
        val arrayOfArray = ValueWrites.traversableWrites[List, List[Int]](writesIntList)
        the[PgSqlClientError] thrownBy arrayOfArray.writes(
          PgType.Int4Array,
          List(List(1)),
          utf8) must have message
          "Type int4 is not an array type and cannot be written as such. Note that this may be because you're trying to write a multi-dimensional array which isn't supported."
      }
    }

    "writesBigDecimal" should simpleSpec[BigDecimal](ValueWrites.writesBigDecimal, PgType.Numeric) {
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
    "writesBoolean" should simpleSpec[Boolean](ValueWrites.writesBoolean, PgType.Bool) {
      case true => Buf.ByteArray(0x01)
      case false => Buf.ByteArray(0x00)
    }
    "writesBuf" should simpleSpec[Buf](ValueWrites.writesBuf, PgType.Bytea) { buf =>
      mkBuf() { bb =>
        bb.put(Buf.ByteArray.Shared.extract(buf))
      }
    }
    "writesByte" should {
      "write int2" should simpleSpec[Byte](ValueWrites.writesByte, PgType.Int2) { byte =>
        mkBuf()(_.putShort(byte.toShort))
      }
      "write int4" should simpleSpec[Byte](ValueWrites.writesByte, PgType.Int4) { byte =>
        mkBuf()(_.putInt(byte.toInt))
      }
      "write int8" should simpleSpec[Byte](ValueWrites.writesByte, PgType.Int8) { byte =>
        mkBuf()(_.putLong(byte.toLong))
      }
    }
    "writesDouble" should simpleSpec[Double](ValueWrites.writesDouble, PgType.Float8) { double =>
      mkBuf()(_.putDouble(double))
    }
    "writesFloat" should {
      "writes float4" should simpleSpec[Float](ValueWrites.writesFloat, PgType.Float4) { float =>
        mkBuf()(_.putFloat(float))
      }
      "writes float8" should simpleSpec[Float](ValueWrites.writesFloat, PgType.Float8) { float =>
        mkBuf()(_.putDouble(float.toDouble))
      }
    }
    "writesInet" should simpleSpec[Inet](ValueWrites.writesInet, PgType.Inet) { inet =>
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
    "writesInstant" should simpleSpec[java.time.Instant](
      ValueWrites.writesInstant,
      PgType.Timestamptz,
      PgType.Timestamp
    ) { ts =>
      mkBuf() { bb =>
        val sincePgEpoch = java.time.Duration.between(PgTime.Epoch, ts)
        val secs = sincePgEpoch.getSeconds
        val nanos = sincePgEpoch.getNano
        val micros = secs * 1000000 + nanos / 1000
        bb.putLong(micros)
      }
    }
    "writesInt" should {
      "write int4" should simpleSpec[Int](ValueWrites.writesInt, PgType.Int4) { int =>
        mkBuf()(_.putInt(int))
      }
      "write int8" should simpleSpec[Int](ValueWrites.writesInt, PgType.Int8) { int =>
        mkBuf()(_.putLong(int.toLong))
      }
    }

    "writesJson" should simpleSpec[Json](ValueWrites.writesJson, PgType.Json) { json =>
      mkBuf(json.jsonByteArray.length) { bb =>
        bb.put(json.jsonByteBuffer)
      }
    }

    "writesJsonb" should simpleSpec[Json](ValueWrites.writesJson, PgType.Jsonb) { json =>
      mkBuf(json.jsonByteArray.length + 1) { bb =>
        bb.put(1.toByte).put(json.jsonByteBuffer)
      }
    }

    "writesLocalDate" should simpleSpec[java.time.LocalDate](
      ValueWrites.writesLocalDate,
      PgType.Date) { date =>
      mkBuf() { bb =>
        bb.putInt(ChronoUnit.DAYS.between(PgDate.Epoch, date).toInt)
      }
    }
    "writesLong" should simpleSpec[Long](ValueWrites.writesLong, PgType.Int8) { long =>
      mkBuf()(_.putLong(long))
    }
    "writesShort" should {
      "writes int2" should simpleSpec[Short](ValueWrites.writesShort, PgType.Int2) { short =>
        mkBuf()(_.putShort(short))
      }
      "writes int4" should simpleSpec[Short](ValueWrites.writesShort, PgType.Int4) { short =>
        mkBuf()(_.putInt(short.toInt))
      }
      "writes int8" should simpleSpec[Short](ValueWrites.writesShort, PgType.Int8) { short =>
        mkBuf()(_.putLong(short.toLong))
      }
    }
    "writesString" should simpleSpec[String](
      ValueWrites.writesString,
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
    "writesUuid" should simpleSpec[java.util.UUID](ValueWrites.writesUuid, PgType.Uuid) { uuid =>
      mkBuf() { bb =>
        bb.putLong(uuid.getMostSignificantBits)
        bb.putLong(uuid.getLeastSignificantBits)
      }
    }
  }
}
