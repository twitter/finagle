package com.twitter.finagle.redis.protocol

import com.twitter.finagle.redis.MissingInstances
import com.twitter.finagle.util.BufReader
import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class StageTest extends FunSuite with GeneratorDrivenPropertyChecks with MissingInstances {

  import Stage.NextStep

  test("readBytes") {
    def stage(count: Int): Stage =
      Stage.readBytes(count)(bytes => NextStep.Emit(BulkReply(bytes)))

    forAll(genBuf) { buf =>
      val NextStep.Emit(BulkReply(a)) = stage(buf.length)(BufReader(buf))
      val incomplete = stage(buf.length + 1)(BufReader(buf))

      assert(a == buf)
      assert(incomplete == NextStep.Incomplete)
    }
  }

  test("readLine") {
    val stage = Stage.readLine(line => NextStep.Emit(StatusReply(line)))

    forAll(genNonEmptyString) { s =>
      val NextStep.Emit(StatusReply(a)) = stage(BufReader(Buf.Utf8(s + "\n")))
      val NextStep.Emit(StatusReply(b)) = stage(BufReader(Buf.Utf8(s + "\r\n")))

      assert(a == b)
      assert(a == s)
    }
  }
}
