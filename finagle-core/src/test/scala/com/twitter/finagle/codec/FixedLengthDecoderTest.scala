package com.twitter.finagle.codec

import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class FixedLengthDecoderTest extends FunSuite with GeneratorDrivenPropertyChecks {

  def stringDecoder(frameSize: Int) =
    new FixedLengthDecoder(frameSize, Buf.Utf8.unapply(_).getOrElse("????"))

  test("FixedLengthDecoder can frame a series of buffers") {
    val decode = stringDecoder(4)

    // the decoder sees one four-byte frame which is returned.
    // three bytes are buffered.
    val a = decode(Buf.Utf8("hi2uABC"))
    assert(a.toList == Seq("hi2u"))

    // on the subsequent byte, the new 4-byte string is returned.
    val b = decode(Buf.Utf8("D"))
    assert(b.toList == Seq("ABCD"))

    val c = decode(Buf.Utf8("0123456789"))
    assert(c.toList == Seq("0123", "4567"))
  }

  test("framing") {
    forAll(
      Gen.alphaStr,
      Gen.posNum[Int]
    ) { (s: String, frameSize: Int) =>
      val decode = stringDecoder(frameSize)
      val buf = Buf.Utf8(s)
      val frames: Seq[String] = decode(buf).toList
      val groupedString = s.grouped(frameSize).toList

      if (buf.length < frameSize) assert(frames == Nil)
      else if (buf.length % frameSize == 0) assert(groupedString == frames)
      else assert(groupedString.take(groupedString.length - 1) == frames)
    }
  }
}
