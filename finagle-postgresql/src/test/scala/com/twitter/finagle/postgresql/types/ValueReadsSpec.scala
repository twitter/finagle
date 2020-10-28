package com.twitter.finagle.postgresql.types

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.MalformedInputException
import java.nio.charset.StandardCharsets

import com.twitter.finagle.postgresql.PgSqlClientError
import com.twitter.finagle.postgresql.PgSqlSpec
import com.twitter.finagle.postgresql.PropertiesSpec
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

class ValueReadsSpec extends PgSqlSpec with PropertiesSpec {

  val utf8 = StandardCharsets.UTF_8

  def mkBuf(capacity: Int = 1024)(f: ByteBuffer => ByteBuffer): Buf = {
    val bb = ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN)
    f(bb)
    bb.flip()
    Buf.ByteBuffer.Owned(bb)
  }

  def acceptFragments(reads: ValueReads[_], accept: PgType, accepts: PgType*): Fragments = {
    val typeFragments = (accept +: accepts).map { tpe =>
      s"accept the ${tpe.name} type" in {
        reads.accepts(accept) must beTrue
      }
    }

    fragments(typeFragments)
  }

  def readsFragment[T: Arbitrary](reads: ValueReads[T], accept: PgType)(encode: T => Buf): Fragment =
    s"read non-null value" in prop { value: T =>
      val ret = reads.reads(accept, WireValue.Value(encode(value)), utf8).asScala
      ret must beSuccessfulTry(value)
    }
  def arrayReadsFragment[T: Arbitrary](reads: ValueReads[T], accept: PgType)(encode: T => Buf) = {
    val arrayReads = ValueReads.traversableReads[List, T](reads, implicitly)
    s"read one-dimensional array of non-null values" in prop { values: List[T] =>
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
      val ret = arrayReads.reads(arrayType, arrayWire, utf8).asScala
      ret must beSuccessfulTry(values)
    }.setGen(Gen.listOfN(5, Arbitrary.arbitrary[T])) // limit to 5 elements to speed things up
  }
  def nonNullableFragment(reads: ValueReads[_], accept: PgType): Fragment =
    s"fail to read a null value" in {
      reads.reads(accept, WireValue.Null, utf8).asScala must beFailedTry
    }

  def nullableFragment[T: Arbitrary](reads: ValueReads[T], accept: PgType): Fragment =
    "is nullable when wrapped in Option" in {
      ValueReads.optionReads(reads).reads(accept, WireValue.Null, utf8).asScala must beSuccessfulTry(beNone)
    }

  def simpleSpec[T: Arbitrary](reads: ValueReads[T], accept: PgType, accepts: PgType*)(encode: T => Buf): Fragments =
    acceptFragments(reads, accept, accepts: _*)
      .append(
        Fragments(
          readsFragment(reads, accept)(encode),
          arrayReadsFragment(reads, accept)(encode),
          nonNullableFragment(reads, accept),
          nullableFragment(reads, accept),
        )
      )

  "ValueReads" should {
    "readsBigDecimal" should simpleSpec[BigDecimal](ValueReads.readsBigDecimal, PgType.Numeric) { bd =>
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
    "readsByte" should simpleSpec[Byte](ValueReads.readsByte, PgType.Char)(byte => Buf.ByteArray(byte))
    "readsDouble" should simpleSpec[Double](ValueReads.readsDouble, PgType.Float8) { double =>
      mkBuf() { bb =>
        bb.putDouble(double)
      }
    }
    "readsFloat" should simpleSpec[Float](ValueReads.readsFloat, PgType.Float4) { float =>
      mkBuf() { bb =>
        bb.putFloat(float)
      }
    }
    "readsInstant" should simpleSpec[java.time.Instant](ValueReads.readsInstant, PgType.Timestamptz, PgType.Timestamp) {
      ts =>
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
          val read = ValueReads.readsInstant.reads(PgType.Timestamptz, WireValue.Value(buf), utf8)
          read.asScala must beAFailedTry(beAnInstanceOf[PgSqlClientError])
        }
      failFor("-Infinity", 0x8000000000000000L)
      failFor("Infinity", 0x7fffffffffffffffL)
    }
    "readsInt" should simpleSpec[Int](ValueReads.readsInt, PgType.Int4) { int =>
      mkBuf() { bb =>
        bb.putInt(int)
      }
    }
    "readsJson" should simpleSpec[Json](ValueReads.readsJson, PgType.Json) { json =>
      mkBuf(json.jsonByteArray.length) { bb =>
        bb.put(json.jsonByteBuffer)
      }
    }
    "readsJson" should simpleSpec[Json](ValueReads.readsJson, PgType.Jsonb) { json =>
      mkBuf(json.jsonByteArray.length + 1) { bb =>
        bb.put(1.toByte).put(json.jsonByteBuffer)
      }
    }
    "readsLong" should simpleSpec[Long](ValueReads.readsLong, PgType.Int8) { long =>
      mkBuf() { bb =>
        bb.putLong(long)
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
        val read = ValueReads.readsString.reads(PgType.Text, Buf.ByteArray(0xc3.toByte, 0x28.toByte), utf8)
        read.asScala must beAFailedTry(beAnInstanceOf[MalformedInputException])
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
