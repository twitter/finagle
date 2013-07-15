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
import org.specs.SpecificationWithJUnit

class InterpreterSpec extends SpecificationWithJUnit {
  "Interpreter" should {
    val queues = CacheBuilder.newBuilder()
      .build(new CacheLoader[ChannelBuffer, BlockingDeque[ChannelBuffer]] {
        def load(k: ChannelBuffer) = new LinkedBlockingDeque[ChannelBuffer]
      })
    val interpreter = new Interpreter(queues)

    "set & get" in {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Get("name")) mustEqual
        Values(Seq(Value("name", "rawr")))
    }

    "transactions" in {
      "set & get/open & get/open" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Open("name"))
        interpreter(Open("name")) must throwA[InvalidStateTransition]
      }

      "set & get/abort" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Abort("name")) must throwA[InvalidStateTransition]
      }

      "set & get/open & get/close" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Open("name")) mustEqual
          Values(Seq(Value("name", "rawr")))
        interpreter(Close("name")) mustEqual Values(Seq())
        interpreter(Open("name")) mustEqual Values(Seq())
      }

      "set & get/open & get/abort" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Open("name")) mustEqual
          Values(Seq(Value("name", "rawr")))
        interpreter(Abort("name")) mustEqual Values(Seq())
        interpreter(Open("name")) mustEqual
          Values(Seq(Value("name", "rawr")))
      }
    }

    "timeouts" in {
      "set & get/t=1" in {
        interpreter(Get("name", Some(1.millisecond))) mustEqual Values(Seq())
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Get("name", Some(1.second))) mustEqual Values(Seq(Value("name", "rawr")))
      }
    }

    "delete" in {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Delete("name"))
      interpreter(Get("name")) mustEqual Values(Seq.empty)
    }

    "flush" in {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Flush("name"))
      interpreter(Get("name")) mustEqual Values(Seq.empty)
    }

    "flushAll" in {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(FlushAll())
      interpreter(Get("name")) mustEqual Values(Seq.empty)
    }
  }
}
