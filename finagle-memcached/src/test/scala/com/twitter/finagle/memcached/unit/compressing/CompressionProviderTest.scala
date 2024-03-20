package com.twitter.finagle.memcached.unit.compressing

import com.twitter.finagle.memcached.compressing.CompressionProvider
import com.twitter.finagle.memcached.compressing.scheme.CompressionScheme
import org.scalacheck.Gen
import java.util.Arrays
import org.scalacheck.Arbitrary.arbitrary
import java.nio.ByteBuffer
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.Return
import com.twitter.util.{Command => _}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class CompressionProviderTest
    extends AnyFunSuite
    with MockitoSugar
    with Matchers
    with ScalaCheckPropertyChecks {

  // Pairwise combinations of all supported compression schemes. Used to test compatibility
  // across implementations.
  private[this] val allCompressionSchemeCombinations =
    for {
      a <- CompressionScheme.All.toSeq
      b <- CompressionScheme.All.toSeq
    } yield {
      (a, b)
    }

  private[this] val allCompressionSchemeCombinationsTable =
    Table(
      ("compressor", "decompressor"),
      allCompressionSchemeCombinations: _*
    )

  // Test data generator. Generates a set of small byte arrays, and then generates a
  // `Buf` that's a concatenation of those. We do this so there is actually some redundancy
  // in the generated data, and the compressor can actually operate. Note that we also continue to
  // get coverage of the "non-compressible" case, because it is possible for this to generate a
  // sequence of a single `Buf` containing random, non-compressible data.
  private val testDataGen: Gen[Buf] =
    for {
      blocks <- Gen.nonEmptyListOf(arbitrary[Array[Byte]])
      bufParts <- Gen.nonEmptyListOf(Gen.oneOf(blocks)).map { parts =>
        // Wrap as byte buffers, and offset them a little bit, to make sure that we handle
        // non-aligned buffers correctly and only compress from the buffer position to its limit.
        parts.map { part =>
          val bb = ByteBuffer.wrap(part)
          if (bb.limit() > 0)
            bb.position(1)
          Buf.ByteBuffer.Shared(bb)
        }
      }
    } yield {
      Buf(bufParts)
    }

  // Test data generator that just returns an array of zeros, for test cases where we must guarantee
  // that the generated test data can be compressed.
  private[this] val alwaysCompressedDataGen =
    Buf.ByteArray.Owned(Array.fill[Byte](1024)(0))

  test("all compression of provider are compatible with each other") {
    // For every pairwise combination of compressing injections, validate that we can compress
    // with one scheme, and decompress the results with any other.

    forAll(allCompressionSchemeCombinationsTable) { (a: CompressionScheme, b: CompressionScheme) =>
      val factoryA = CompressionProvider(a, NullStatsReceiver)
      val factoryB = CompressionProvider(b, NullStatsReceiver)
      forAll(testDataGen) { buf =>
        withClue((a, b)) {
          val compressed = factoryA.compressor(buf)
          val decompressedTry = factoryB.decompressor(compressed)
          val result = decompressedTry.map { b2 =>
            Buf.ByteArray.Owned.extract(b2)
          }

          result match {
            case Return(resultArray) =>
              Arrays.equals(resultArray, Buf.ByteArray.Owned.extract(buf)) shouldBe true
            case _ => throw new Exception("failed to deserialize")
          }
        }
      }
    }
  }

  test("compressing provider returns correct flags") {
    val table = Table("compressor", CompressionScheme.All.toSeq: _*)

    forAll(table) { scheme: CompressionScheme =>
      val factory = CompressionProvider(scheme, NullStatsReceiver)
      forAll(alwaysCompressedDataGen) { buf: Buf =>
        withClue(scheme) {

          val (resultFlag, _) = factory.compressor(buf)

          val expectedFlags =
            scheme.flagSettingId << CompressionScheme.FlagShift

          resultFlag shouldBe expectedFlags
        }
      }
    }
  }
}
