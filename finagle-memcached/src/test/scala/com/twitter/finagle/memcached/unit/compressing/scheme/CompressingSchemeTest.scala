package com.twitter.finagle.memcached.unit.compressing.scheme

import com.twitter.finagle.memcached.compressing.scheme.CompressionScheme
import com.twitter.finagle.memcached.compressing.scheme.Lz4
import com.twitter.finagle.memcached.compressing.scheme.Uncompressed
import org.scalacheck.Arbitrary.arbitrary
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CompressingSchemeTest
    extends AnyFunSuite
    with MockitoSugar
    with Matchers
    with ScalaCheckPropertyChecks {

  test("compressing scheme compressionType") {
    val table = Table("compressor", CompressionScheme.All.toSeq: _*)

    forAll(table) { scheme: CompressionScheme =>
      forAll(arbitrary[Int]) { flags: Int =>
        withClue(scheme) {

          val compressionFlags =
            (flags & ~CompressionScheme.FlagMask) | (scheme.flagSettingId << CompressionScheme.FlagShift)

          CompressionScheme.compressionType(compressionFlags) shouldBe scheme
        }
      }
    }
  }

  test("compressing scheme returns correct compression flags") {
    val table = Table(("compressor", "flags"), (Uncompressed, 0), (Lz4, 16))

    forAll(table) { (scheme: CompressionScheme, expectedFlags) =>
      withClue(scheme) {

        scheme.compressionFlags shouldBe expectedFlags
      }
    }
  }

  test("compressing scheme flagsWithCompression returns correct flags") {
    val table = Table("compressor", CompressionScheme.All.toSeq: _*)

    forAll(table) { scheme: CompressionScheme =>
      forAll(arbitrary[Int]) { flags: Int =>
        withClue(scheme) {

          val compressionFlag = scheme.compressionFlags

          val resultFlag =
            CompressionScheme.flagsWithCompression(flags, compressionFlag)

          val expectedFlags =
            (flags & ~CompressionScheme.FlagMask) | (scheme.flagSettingId << CompressionScheme.FlagShift)

          resultFlag shouldBe expectedFlags
        }
      }
    }
  }
}
