package com.twitter.finagle.mux

import com.twitter.io.Buf
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class ReqRepHeadersTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  private val Header: Gen[(Buf, Buf)] = {
    val byte = Gen.choose[Byte](Byte.MinValue, Byte.MaxValue)
    val token = Gen.listOf(byte).map(bs => Buf.ByteArray(bs: _*))
    for {
      k <- token
      v <- token
    } yield (k -> v)
  }

  test("round trips headers") {
    forAll(Gen.listOf(Header)) { hs =>
      val encoded = ReqRepHeaders.encode(hs)
      val decoded = ReqRepHeaders.decode(encoded)

      if (ContextCodec.encodedLength(hs.iterator) <= ReqRepHeaders.MaxEncodedLength) {
        assert(decoded == hs)
      } else {
        assert(decoded == truncate(hs))
      }
    }
  }

  def truncate(headers: Iterable[(Buf, Buf)]): Iterable[(Buf, Buf)] = {
    var size = 0
    headers.takeWhile {
      case (k, v) =>
        size += 4 + k.length + v.length
        size <= ReqRepHeaders.MaxEncodedLength
    }
  }

  test("encodes up to MaxEncodedLength") {
    // the key, value pair should max out the encoded header block size.
    val k = Buf.Utf8("k")
    val v = Buf.Utf8("." * (ReqRepHeaders.MaxEncodedLength - 5))

    val encoded = ReqRepHeaders.encode(Seq(k -> v))
    assert(encoded.length == ReqRepHeaders.MaxEncodedLength)
    assert(ReqRepHeaders.decode(encoded) == Seq(k -> v))
  }

  test("drops headers that overflow the max encoded size") {
    // the key, value pair should max out the encoded header block size.
    val k = Buf.Utf8("k")
    val v1 = Buf.Utf8("1" * (ReqRepHeaders.MaxEncodedLength - 5))
    val v2 = Buf.Utf8("2")

    val encoded1 = ReqRepHeaders.encode(Seq(k -> v1, k -> v2))
    assert(encoded1.length == ReqRepHeaders.MaxEncodedLength) // should have dropped the second pair
    assert(ReqRepHeaders.decode(encoded1) == Seq(k -> v1))

    val encoded2 = ReqRepHeaders.encode(Seq(k -> v2, k -> v1))
    assert(encoded2.length == 6) // should have dropped the second pair
    assert(ReqRepHeaders.decode(encoded2) == Seq(k -> v2))
  }

}
