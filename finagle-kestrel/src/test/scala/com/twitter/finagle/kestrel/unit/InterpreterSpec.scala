package com.twitter.finagle.kestrel.unit

import org.specs.Specification
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.conversions.time._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.kestrel.Interpreter
import com.twitter.util.StateMachine.InvalidStateTransition
import com.twitter.util.{MapMaker, Time}
import java.util.concurrent.{BlockingDeque, LinkedBlockingDeque}

object InterpreterSpec extends Specification {
  "Interpreter" should {
    val queues = MapMaker[ChannelBuffer, BlockingDeque[ChannelBuffer]] { config =>
      config.compute { key =>
        new LinkedBlockingDeque[ChannelBuffer]
      }
    }
    val interpreter = new Interpreter(queues)

    "set & get" in {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Get("name", collection.Set.empty)) mustEqual
        Values(Seq(Value("name", "rawr")))
    }

    "transactions" in {
      "set & get/open & get/open" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Get("name", collection.Set(Open())))
        interpreter(Get("name", collection.Set(Open()))) must throwA[InvalidStateTransition]
      }

      "set & get/abort" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Get("name", collection.Set(Abort()))) must throwA[InvalidStateTransition]
      }

      "set & get/open & get/open/abort" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Get("name", collection.Set(Open())))
        interpreter(Get("name", collection.Set(Open(), Abort()))) must throwA[InvalidStateTransition]
      }

      "set & get/open & get/close" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Get("name", collection.Set(Open()))) mustEqual
          Values(Seq(Value("name", "rawr")))
        interpreter(Get("name", collection.Set(Close()))) mustEqual Values(Seq())
        interpreter(Get("name", collection.Set(Open()))) mustEqual Values(Seq())
      }

      "set & get/open & get/abort" in {
        interpreter(Set("name", Time.now, "rawr"))
        interpreter(Get("name", collection.Set(Open()))) mustEqual
          Values(Seq(Value("name", "rawr")))
        interpreter(Get("name", collection.Set(Abort()))) mustEqual Values(Seq())
        interpreter(Get("name", collection.Set(Open()))) mustEqual
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
      interpreter(Get("name", collection.Set.empty)) mustEqual Values(Seq.empty)
    }

    "flush" in {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(Flush("name"))
      interpreter(Get("name", collection.Set.empty)) mustEqual Values(Seq.empty)
    }

    "flushAll" in {
      interpreter(Set("name", Time.now, "rawr"))
      interpreter(FlushAll())
      interpreter(Get("name", collection.Set.empty)) mustEqual Values(Seq.empty)
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