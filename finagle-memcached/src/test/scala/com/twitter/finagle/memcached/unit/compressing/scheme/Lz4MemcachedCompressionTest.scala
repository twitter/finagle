package com.twitter.finagle.memcached.unit.compressing.scheme

import com.twitter.finagle.memcached.compressing.scheme.Lz4MemcachedCompression
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.io.Buf
import com.twitter.util.{Command => _}
import java.nio.ByteBuffer
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class Lz4MemcachedCompressionTest
    extends AnyFunSuite
    with MockitoSugar
    with Matchers
    with BeforeAndAfter
    with ScalaCheckPropertyChecks {

  val stats = new InMemoryStatsReceiver

  val lz4MemcachedCompression = Lz4MemcachedCompression(stats)

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

  // Test data generator that just returns an array of zeros, for test cases where we must guarantee
  // that the generated test data should not be compressed.
  private[this] val neverCompressedDataGen =
    Buf.ByteArray.Owned(Array.fill[Byte](10)(0))

  val identity =
    lz4MemcachedCompression.andThen(lz4MemcachedCompression.inverse)

  after {
    stats.clear()
  }

  test("Compressed values get decompressed correctly") {
    assert(alwaysCompressedDataGen === identity(alwaysCompressedDataGen))
  }

  test("compressor is working properly") {
    val table =
      Table(
        ("data", "expectedFlags", "shouldCompress"),
        (alwaysCompressedDataGen, 16, true),
        (neverCompressedDataGen, 0, false))

    forAll(table) { (data, expectedFlags, shouldCompress) =>
      val (flags, buf) = lz4MemcachedCompression.apply(data)

      if (shouldCompress) {
        assert(buf.length < data.length)
        assert(flags == expectedFlags)
      } else {
        assert(buf.length == data.length)
        assert(flags == expectedFlags)
      }
    }

    assert(stats.counters(Seq("lz4", "compression", "attempted")) == 1)
    assert(stats.counters(Seq("lz4", "compression", "skipped")) == 1)
  }

  test("stats receiver records values correctly") {
    val data1 =
      Buf.ByteArray.Owned(Array.fill[Byte](1024)(0))
    val data2 =
      Buf.ByteArray.Owned(Array.fill[Byte](1045)(0))
    val table =
      Table("data", data1, data2)

    forAll(table) { data =>
      val _ = identity(data)
    }
    assert(stats.counters(Seq("lz4", "compression", "attempted")) == 2)
    assert(stats.counters(Seq("lz4", "decompression", "attempted")) == 2)
    assert(!stats.counters.contains(Seq("lz4", "compression", "skipped")))
  }

  test("throws error if decompression receives wrong flags") {
    val err = intercept[IllegalStateException] {
      lz4MemcachedCompression.invert((0, neverCompressedDataGen))
    }

    assert(err.getMessage.nonEmpty)
  }
}
