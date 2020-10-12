package com.twitter.finagle.postgresql.types

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.charset.StandardCharsets

import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Buf
import org.scalacheck.Arbitrary
import org.specs2.mutable.Specification
import org.specs2.specification.core.Fragment
import org.specs2.specification.core.Fragments

class ValueReadsSpec extends Specification with PropertiesSpec {

  val utf8 = StandardCharsets.UTF_8

  def mkBuf(capacity: Int = 1024)(f: ByteBuffer => ByteBuffer): Buf = {
    val bb = ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN)
    f(bb)
    bb.flip()
    Buf.ByteBuffer.Owned(bb)
  }

  def acceptFragments(reads: ValueReads[_], accept: PgType, accepts: PgType*): Fragments = {
    val fragments = (accept +: accepts).map { tpe =>
      s"accept the ${tpe.name} type" in {
        reads.accepts(accept) must beTrue
      }
    }

    Fragments(fragments: _*)
  }

  def readsFragment[T: Arbitrary](reads: ValueReads[T], accept: PgType)(encode: T => Buf): Fragment = {
    s"read non-null value" in prop { value: T =>
      val ret = reads.reads(accept, WireValue.Value(encode(value)), utf8).asScala
      ret must beSuccessfulTry(value)
    }
  }
  def nonNullableFragment(reads: ValueReads[_], accept: PgType): Fragment =
    s"fail to read a null value" in {
      reads.reads(accept, WireValue.Null, utf8).asScala must beFailedTry
    }

  def simpleSpec[T: Arbitrary](reads: ValueReads[T], accept: PgType, accepts: PgType*)(encode: T => Buf): Fragments =
    acceptFragments(reads, accept, accepts: _*)
      .append(
        Fragments(
          readsFragment(reads, accept)(encode),
          nonNullableFragment(reads, accept),
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
