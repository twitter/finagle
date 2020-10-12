package com.twitter.finagle.postgresql.types

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

import com.twitter.finagle.postgresql.PgSqlSpec
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Types.Oid
import com.twitter.finagle.postgresql.Types.PgArray
import com.twitter.finagle.postgresql.Types.PgArrayDim
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.transport.PgBuf
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
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

  def readsFragment[T: Arbitrary](reads: ValueReads[T], accept: PgType)(encode: T => Buf): Fragment = {
    s"read non-null value" in prop { value: T =>
      val ret = reads.reads(accept, WireValue.Value(encode(value)), utf8).asScala
      ret must beSuccessfulTry(value)
    }
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
      // TODO: it'd be better to take the actual array type here...
      val arrayPgType = PgType(name = "fake", oid = Oid(0), Kind.Array(accept))
      val ret = arrayReads.reads(arrayPgType, arrayWire, utf8).asScala
      ret must beSuccessfulTry(values)
    }
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
    "readsBoolean" should simpleSpec[Boolean](ValueReads.readsBoolean, PgType.Bool) {
      case true => Buf.ByteArray(0x01)
      case false => Buf.ByteArray(0x00)
    }
    "readsByte" should simpleSpec[Byte](ValueReads.readsByte, PgType.Char) { byte => Buf.ByteArray(byte) }
    "readsShort" should simpleSpec[Short](ValueReads.readsShort, PgType.Int2) { short =>
      mkBuf() { bb =>
        bb.putShort(short)
      }
    }
    "readsInt" should simpleSpec[Int](ValueReads.readsInt, PgType.Int4) { int =>
      mkBuf() { bb =>
        bb.putInt(int)
      }
    }
    "readsLong" should simpleSpec[Long](ValueReads.readsLong, PgType.Int8) { long =>
      mkBuf() { bb =>
        bb.putLong(long)
      }
    }
    "readsString" should simpleSpec[String](ValueReads.readsString, PgType.Text, PgType.Varchar, PgType.Bpchar, PgType.Name, PgType.Unknown) { string =>
      mkBuf() { bb =>
        bb.put(string.getBytes(utf8))
      }
    }
    "readsBuf" should simpleSpec[Buf](ValueReads.readsBuf, PgType.Bytea) { buf =>
      mkBuf() { bb =>
        bb.put(Buf.ByteArray.Shared.extract(buf))
      }
    }
  }
}
