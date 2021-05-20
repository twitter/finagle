package com.twitter.finagle.memcached.unit.protocol.text.client

import com.twitter.finagle.memcached.protocol.text.client.AbstractCommandToBuf
import com.twitter.io.BufByteWriter
import java.nio.charset.StandardCharsets
import org.scalacheck.Gen
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class CommandToBufTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  test("`commandToBuf.lengthAsString` returns same value as `Integer.toString.length`") {
    forAll(Gen.posNum[Int]) { i: Int =>
      assert(AbstractCommandToBuf.lengthAsString(i) == i.toString.length)
    }
  }

  test("`commandToBuf.writeDigits` produces same buffer as `BufByteWriter.writeString`") {
    forAll(Gen.posNum[Int]) { i: Int =>
      val bw1 = BufByteWriter.fixed(16)
      val bw2 = BufByteWriter.fixed(16)
      AbstractCommandToBuf.writeDigits(i, bw1)
      bw2.writeString(i.toString, StandardCharsets.US_ASCII)
      val buf1 = bw1.owned()
      val buf2 = bw2.owned()
      assert(buf1.equals(buf2))
    }
  }
}
