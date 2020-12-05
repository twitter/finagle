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
import org.specs2.specification.core.Fragment
import org.specs2.specification.core.Fragments

class ValueWritesSpec extends PgSqlSpec with PropertiesSpec {

  val utf8 = StandardCharsets.UTF_8

  def mkBuf(capacity: Int = 1024)(f: ByteBuffer => ByteBuffer): Buf = {
    val bb = ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN)
    f(bb)
    bb.flip()
    Buf.ByteBuffer.Owned(bb)
  }

  def acceptFragments(writes: ValueWrites[_], accept: PgType, accepts: PgType*): Fragments = {
    val typeFragments = (accept +: accepts).map { tpe =>
      s"accept the ${tpe.name} type" in {
        writes.accepts(accept) must beTrue
      }
    }

    fragments(typeFragments)
  }

  def writesFragment[T: Arbitrary](writes: ValueWrites[T], accept: PgType)(encode: T => Buf): Fragment =
    s"write non-null value" in prop { value: T =>
      val ret = writes.writes(accept, value, utf8)
      ret must_== WireValue.Value(encode(value))
    }

  def arrayWritesFragment[T: Arbitrary](writes: ValueWrites[T], accept: PgType)(encode: T => Buf) = {
    val arrayWrites = ValueWrites.traversableWrites[List, T](writes)
    s"write one-dimensional array of non-null values" in prop { values: List[T] =>
      val data = values.map(v => encode(v)).map(WireValue.Value).toIndexedSeq
      val pgArray = PgArray(
        dimensions = 1,
        dataOffset = 0,
        elemType = accept.oid,
        arrayDims = IndexedSeq(PgArrayDim(values.length, 1)),
        data = data,
      )
      val arrayWire = WireValue.Value(PgBuf.writer.array(pgArray).build)
      val arrayType = PgType.arrayOf(accept).getOrElse(sys.error(s"no array type for ${accept.name}"))
      val ret = arrayWrites.writes(arrayType, values, utf8)
      ret must_== arrayWire
    }.setGen(Gen.listOfN(5, Arbitrary.arbitrary[T])) // limit to 5 elements to speed things up
  }

  def nullableFragment[T: Arbitrary](writes: ValueWrites[T], accept: PgType): Fragment =
    "is nullable when wrapped in Option" in {
      ValueWrites.optionWrites(writes).writes(accept, None, utf8) must_== WireValue.Null
    }

  def simpleSpec[T: Arbitrary](writes: ValueWrites[T], accept: PgType, accepts: PgType*)(encode: T => Buf): Fragments =
    acceptFragments(writes, accept, accepts: _*)
      .append(
        Fragments(
          writesFragment(writes, accept)(encode),
          arrayWritesFragment(writes, accept)(encode),
          nullableFragment(writes, accept),
        )
      )

  "ValueWrites" should {

    "by" should {
      "accept the underlying type" in {
        val intByLong = ValueWrites.by[Long, Int](_.toLong)
        intByLong.accepts(PgType.Int4) must beFalse
        intByLong.accepts(PgType.Int8) must beTrue
      }
      "write the underlying value" in prop { value: Int =>
        val intByLong = ValueWrites.by[Long, Int](_.toLong)
        val wrote = intByLong.writes(PgType.Int8, value, utf8)
        wrote must_== WireValue.Value(Buf.U64BE(value.toLong))
      }
    }

    "optionWrites" should {
      "delegate writes when Some" in {
        val optionalInt = ValueWrites.optionWrites(ValueWrites.writesInt)
        val wrote = optionalInt.writes(PgType.Int4, Some(0), utf8)
        wrote must_== ValueWrites.writesInt.writes(PgType.Int4, 0, utf8)
      }
      "accept the underlying type" in {
        val optionalInt = ValueWrites.optionWrites(ValueWrites.writesInt)
        optionalInt.accepts(PgType.Int4) must beTrue
        optionalInt.accepts(PgType.Text) must beFalse
      }
      "write Null when None" in {
        val optionalInt = ValueWrites.optionWrites(ValueWrites.writesInt)
        val wrote = optionalInt.writes(PgType.Int4, None, utf8)
        wrote must_== WireValue.Null
      }
    }

    "traversableWrites" should {
      "accept the underlying type" in {
        val writesIntList = ValueWrites.traversableWrites[List, Int](ValueWrites.writesInt)
        writesIntList.accepts(PgType.Int4Array) must beTrue
        writesIntList.accepts(PgType.Int4) must beFalse
        writesIntList.accepts(PgType.Int2Array) must beFalse
      }

      "reject non-array types when reading" in {
        val writesIntList = ValueWrites.traversableWrites[List, Int](ValueWrites.writesInt)
        writesIntList.writes(PgType.Int4, Nil, utf8) must throwA[PgSqlClientError](
          s"Type int4 is not an array type and cannot be written as such."
        )
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
        writesIntList.writes(PgType.Int4Array, Nil, utf8) must_== WireValue.Value(arrayBuf)
      }

      "fail for multi-dimensional arrays" in {
        val writesIntList = ValueWrites.traversableWrites[List, Int](ValueWrites.writesInt)
        val arrayOfArray = ValueWrites.traversableWrites[List, List[Int]](writesIntList)
        arrayOfArray.writes(PgType.Int4Array, List(List(1)), utf8) must throwA[PgSqlClientError](
          "Type int4 is not an array type and cannot be written as such. Note that this may be because you're trying to write a multi-dimensional array which isn't supported."
        )
      }
    }

    "writesBigDecimal" should simpleSpec[BigDecimal](ValueWrites.writesBigDecimal, PgType.Numeric) { bd =>
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
    "writesByte" should simpleSpec[Byte](ValueWrites.writesByte, PgType.Int2) { byte =>
      mkBuf() { bb =>
        bb.putShort(byte.toShort)
      }
    }
    "writesDouble" should simpleSpec[Double](ValueWrites.writesDouble, PgType.Float8) { double =>
      mkBuf() { bb =>
        bb.putDouble(double)
      }
    }
    "writesFloat" should simpleSpec[Float](ValueWrites.writesFloat, PgType.Float4) { float =>
      mkBuf() { bb =>
        bb.putFloat(float)
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
    ) {
      ts =>
        mkBuf() { bb =>
          val sincePgEpoch = java.time.Duration.between(PgTime.Epoch, ts)
          val secs = sincePgEpoch.getSeconds
          val nanos = sincePgEpoch.getNano
          val micros = secs * 1000000 + nanos / 1000
          bb.putLong(micros)
        }
    }
    "writesInt" should simpleSpec[Int](ValueWrites.writesInt, PgType.Int4) { int =>
      mkBuf() { bb =>
        bb.putInt(int)
      }
    }
    "writesJson" should simpleSpec[Json](ValueWrites.writesJson, PgType.Json) { json =>
      mkBuf(json.jsonByteArray.length) { bb =>
        bb.put(json.jsonByteBuffer)
      }
    }
    "writesJson" should simpleSpec[Json](ValueWrites.writesJson, PgType.Jsonb) { json =>
      mkBuf(json.jsonByteArray.length + 1) { bb =>
        bb.put(1.toByte).put(json.jsonByteBuffer)
      }
    }
    "writesLocalDate" should simpleSpec[java.time.LocalDate](ValueWrites.writesLocalDate, PgType.Date) { date =>
      mkBuf() { bb =>
        bb.putInt(ChronoUnit.DAYS.between(PgDate.Epoch, date).toInt)
      }
    }
    "writesLong" should simpleSpec[Long](ValueWrites.writesLong, PgType.Int8) { long =>
      mkBuf() { bb =>
        bb.putLong(long)
      }
    }
    "writesShort" should simpleSpec[Short](ValueWrites.writesShort, PgType.Int2) { short =>
      mkBuf() { bb =>
        bb.putShort(short)
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
