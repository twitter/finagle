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
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InterpreterTest extends FunSuite {
  trait InterpreterHelper {
    val queues = CacheBuilder.newBuilder()
      .build(new CacheLoader[ChannelBuffer, BlockingDeque[ChannelBuffer]] {
      def load(k: ChannelBuffer) = new LinkedBlockingDeque[ChannelBuffer]
    })
    val interpreter = new Interpreter(queues)
  }

  test("Interpreter should set & get") {
    new InterpreterHelper {
      interpreter(Set("name", Time.now, "rawr"))
      assert(interpreter(Get("name")) ===
        Values(Seq(Value("name", "rawr"))))
    }
  }

  test("Interpreter: transactions should set & get/open & get/open") {
    new InterpreterHelper {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Open("name"))
      intercept[InvalidStateTransition] {
        interpreter(Open("name"))
      }
    }
  }

  test("Interpreter: transactions should set & get/abort") {
    new InterpreterHelper {
      interpreter(Set("name", Time.now, "rawr"))
      intercept[InvalidStateTransition] {
        interpreter(Abort("name"))
      }
    }
  }

  test("Interpreter: transactions should set & get/open & get/close") {
    new InterpreterHelper {
      interpreter(Set("name", Time.now, "rawr"))
      assert(interpreter(Open("name")) ===
        Values(Seq(Value("name", "rawr"))))
      assert(interpreter(Close("name")) === Values(Seq()))
      assert(interpreter(Open("name")) === Values(Seq()))
    }
  }

  test("Interpreter: transactions should set & get/open & get/abort") {
    new InterpreterHelper {
      interpreter(Set("name", Time.now, "rawr"))
      assert(interpreter(Open("name")) ===
        Values(Seq(Value("name", "rawr"))))
      assert(interpreter(Abort("name")) === Values(Seq()))
      assert(interpreter(Open("name")) ===
        Values(Seq(Value("name", "rawr"))))
    }
  }

  test("Interpreter: timeouts: set & get/t=1") {
    new InterpreterHelper {
      assert(interpreter(Get("name", Some(1.millisecond))) === Values(Seq()))
      interpreter(Set("name", Time.now, "rawr"))
      assert(interpreter(Get("name", Some(1.second))) === Values(Seq(Value("name", "rawr"))))
    }
  }

  test("Interpreter: delete") {
    new InterpreterHelper {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Delete("name"))
      assert(interpreter(Get("name")) === Values(Seq.empty))
    }
  }

  test("Interpreter: flush") {
    new InterpreterHelper {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Flush("name"))
      assert(interpreter(Get("name")) === Values(Seq.empty))
    }
  }

  test("Interpreter: flushAll") {
    new InterpreterHelper {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(FlushAll())
      assert(interpreter(Get("name")) === Values(Seq.empty))
    }
  }
}
