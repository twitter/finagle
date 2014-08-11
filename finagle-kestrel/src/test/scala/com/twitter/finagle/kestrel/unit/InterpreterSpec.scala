package com.twitter.finagle.kestrel.unit

import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.twitter.conversions.time._
import com.twitter.finagle.kestrel.Interpreter
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.StateMachine.InvalidStateTransition
import com.twitter.util.Time
import java.util.concurrent.{BlockingDeque, LinkedBlockingDeque}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Suites}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InterpreterTest extends Suites(
  new InterpreterTests,
  new DecodingCommandTests
)

class InterpreterTests extends FunSuite {
  val queues = CacheBuilder.newBuilder()
    .build(new CacheLoader[ChannelBuffer, BlockingDeque[ChannelBuffer]] {
      def load(k: ChannelBuffer) = new LinkedBlockingDeque[ChannelBuffer]
    })
  val interpreter = new Interpreter(queues)

  test("set & get")  {
    interpreter(Set("name", Time.now, "rawr"))
    assert(interpreter(Get("name")) === Values(Seq(Value("name", "rawr"))))
  }

  test("transactions") {
    test("set & get/open & get/open") {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Open("name"))
      intercept[InvalidStateTransition] {
        interpreter(Open("name"))
      }
    }

    test("set & get/abort") {
      interpreter(Set("name", Time.now, "rawr"))
      intercept[InvalidStateTransition] {
        interpreter(Abort("name"))
      }
    }

    test("set & get/open & get/close") {
      interpreter(Set("name", Time.now, "rawr"))
      assert(interpreter(Open("name")) === Values(Seq(Value("name", "rawr"))))
      assert(interpreter(Close("name")) === Values(Seq()))
      assert(interpreter(Open("name")) === Values(Seq()))
    }

    test("set & get/open & get/abort") {
      interpreter(Set("name", Time.now, "rawr"))
      assert(interpreter(Open("name")) === Values(Seq(Value("name", "rawr"))))
      assert(interpreter(Abort("name")) === Values(Seq()))
      assert(interpreter(Open("name")) === Values(Seq(Value("name", "rawr"))))
    }
  }

  test("timeouts") {
    test("set & get/t=1") {
      assert(interpreter(Get("name", Some(1.millisecond))) === Values(Seq()))
      interpreter(Set("name", Time.now, "rawr"))
      assert(interpreter(Get("name", Some(1.second))) === Values(Seq(Value("name", "rawr"))))
    }
  }

  test("delete") {
    interpreter(Set("name", Time.now, "rawr"))
    interpreter(Delete("name"))
    assert(interpreter(Get("name")) === Values(Seq.empty))
  }

  test("flush") {
    interpreter(Set("name", Time.now, "rawr"))
    interpreter(Flush("name"))
    assert(interpreter(Get("name")) === Values(Seq.empty))
  }

  test("flushAll") {
    interpreter(Set("name", Time.now, "rawr"))
    interpreter(FlushAll())
    assert(interpreter(Get("name")) === Values(Seq.empty))
  }
}

class DecodingCommandTests extends FunSuite {
    val dtc = new DecodingToCommand
    def getCmdSeq(subCmd: String) = {
      dtc.parseNonStorageCommand(
        Seq(copiedBuffer("get".getBytes),
            copiedBuffer(subCmd.getBytes)))
    }

    test("parse get with timeout") {
      assert(getCmdSeq("foo/t=123") === Get(copiedBuffer("foo".getBytes), Some(123.milliseconds)))
    }

    test("parse close/open with timeout") {
      assert(getCmdSeq("foo/close/open/t=123") === CloseAndOpen(copiedBuffer("foo".getBytes), Some(123.milliseconds)))
    }

    test("parse close/open with timeout in between") {
      assert(getCmdSeq("foo/t=123/close/open") === CloseAndOpen(copiedBuffer("foo".getBytes), Some(123.milliseconds)))
    }

    test("parse without timeout") {
      assert(getCmdSeq("foo") === Get(copiedBuffer("foo".getBytes), None))
    }
}
