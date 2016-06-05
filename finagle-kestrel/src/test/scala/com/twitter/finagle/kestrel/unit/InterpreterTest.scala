package com.twitter.finagle.kestrel.unit

import java.util.concurrent.{BlockingDeque, LinkedBlockingDeque}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.conversions.time._
import com.twitter.finagle.kestrel.Interpreter
import com.twitter.finagle.kestrel.protocol._
import com.twitter.io.Buf
import com.twitter.util.StateMachine.InvalidStateTransition
import com.twitter.util.Time


@RunWith(classOf[JUnitRunner])
class InterpreterTest extends FunSuite {
  trait InterpreterHelper {
    val queues = CacheBuilder.newBuilder()
      .build(new CacheLoader[Buf, BlockingDeque[Buf]] {
      def load(k: Buf) = new LinkedBlockingDeque[Buf]
    })
    val interpreter = new Interpreter(queues)
  }

  test("Interpreter should set & get") {
    new InterpreterHelper {
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      assert(interpreter(Get(Buf.Utf8("name"))) ==
        Values(Seq(Value(Buf.Utf8("name"), Buf.Utf8("rawr")))))
    }
  }

  test("Interpreter: transactions should set & get/open & get/open") {
    new InterpreterHelper {
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      interpreter(Open(Buf.Utf8("name")))
      intercept[InvalidStateTransition] {
        interpreter(Open(Buf.Utf8("name")))
      }
    }
  }

  test("Interpreter: transactions should set & get/abort") {
    new InterpreterHelper {
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      intercept[InvalidStateTransition] {
        interpreter(Abort(Buf.Utf8("name")))
      }
    }
  }

  test("Interpreter: transactions should set & get/open & get/close") {
    new InterpreterHelper {
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      assert(interpreter(Open(Buf.Utf8("name"))) ==
        Values(Seq(Value(Buf.Utf8("name"), Buf.Utf8("rawr")))))
      assert(interpreter(Close(Buf.Utf8("name"))) == Values(Seq()))
      assert(interpreter(Open(Buf.Utf8("name"))) == Values(Seq()))
    }
  }

  test("Interpreter: transactions should set & get/open & get/abort") {
    new InterpreterHelper {
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      assert(interpreter(Open(Buf.Utf8("name"))) ==
        Values(Seq(Value(Buf.Utf8("name"), Buf.Utf8("rawr")))))
      assert(interpreter(Abort(Buf.Utf8("name"))) == Values(Seq()))
      assert(interpreter(Open(Buf.Utf8("name"))) ==
        Values(Seq(Value(Buf.Utf8("name"), Buf.Utf8("rawr")))))
    }
  }

  test("Interpreter: timeouts: set & get/t=1") {
    new InterpreterHelper {
      assert(interpreter(Get(Buf.Utf8("name"), Some(1.millisecond))) == Values(Seq()))
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      assert(interpreter(Get(Buf.Utf8("name"), Some(1.second))) == Values(Seq(Value(Buf.Utf8("name"), Buf.Utf8("rawr")))))
    }
  }

  test("Interpreter: delete") {
    new InterpreterHelper {
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      interpreter(Delete(Buf.Utf8("name")))
      assert(interpreter(Get(Buf.Utf8("name"))) == Values(Seq.empty))
    }
  }

  test("Interpreter: flush") {
    new InterpreterHelper {
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      interpreter(Flush(Buf.Utf8("name")))
      assert(interpreter(Get(Buf.Utf8("name"))) == Values(Seq.empty))
    }
  }

  test("Interpreter: flushAll") {
    new InterpreterHelper {
      interpreter(Set(Buf.Utf8("name"), Time.now, Buf.Utf8("rawr")))
      interpreter(FlushAll())
      assert(interpreter(Get(Buf.Utf8("name"))) == Values(Seq.empty))
    }
  }
}
