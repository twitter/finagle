package com.twitter.finagle.kestrel.unit

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.conversions.time._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.kestrel.Interpreter
import com.twitter.util.StateMachine.InvalidStateTransition
import com.twitter.util.{MapMaker, Time}
import java.util.concurrent.{BlockingDeque, LinkedBlockingDeque}

class InterpreterSpec extends SpecificationWithJUnit {
  "Interpreter" should {
    val queues = MapMaker[ChannelBuffer, BlockingDeque[ChannelBuffer]] { config =>
      config.compute { key =>
        new LinkedBlockingDeque[ChannelBuffer]
      }
    }
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

      "set & get/open & get/open/abort" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Open("name"))
        //interpreter(Get("name", collection.Set(Open(), Abort()))) must throwA[InvalidStateTransition]
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

    "version" in {

    }

    "shutDown" in {

    }

    "dumpConfig" in {

    }

    "stats" in {

    }

    "dumpStats" in {

    }
  }
}
