package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.MissingInstances
import com.twitter.io.{Buf, ByteReader}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

class StageTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks with MissingInstances {

  import Stage.NextStep

  test("readBytes") {
    def stage(count: Int): Stage =
      Stage.readBytes(count)(bytes => NextStep.Emit(BulkReply(bytes)))

    forAll(genBuf) { buf =>
      val NextStep.Emit(BulkReply(a)) = stage(buf.length)(ByteReader(buf))
      val incomplete = stage(buf.length + 1)(ByteReader(buf))

      assert(a == buf)
      assert(incomplete == NextStep.Incomplete)
    }
  }

  test("readLine") {
    val stage = Stage.readLine(line => NextStep.Emit(StatusReply(line)))

    forAll(genNonEmptyString) { s =>
      val NextStep.Emit(StatusReply(a)) = stage(ByteReader(Buf.Utf8(s + "\n")))
      val NextStep.Emit(StatusReply(b)) = stage(ByteReader(Buf.Utf8(s + "\r\n")))

      assert(a == b)
      assert(a == s)
    }
  }
}
