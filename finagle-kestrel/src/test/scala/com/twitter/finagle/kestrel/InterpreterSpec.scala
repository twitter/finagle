package com.twitter.finagle.kestrel

import org.specs.Specification
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.conversions.time._
import org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer
import org.jboss.netty.buffer.ChannelBuffer
import java.util.concurrent.LinkedBlockingQueue

object InterpreterSpec extends Specification {
  "Interpreter" should {
    val interpreter = new Interpreter(() => new LinkedBlockingQueue[ChannelBuffer])

    "set & get" in {
      interpreter(Set("name", 0, 0.seconds, "rawr"))
      interpreter(Get("name", collection.Set.empty)) mustEqual
        Values(Seq(Value("name", "rawr")))
    }

    "delete" in {
      interpreter(Set("name", 0, 0.seconds, "rawr"))
      interpreter(Delete("name"))
      interpreter(Get("name", collection.Set.empty)) mustEqual Values(Seq.empty)
    }

    "flush" in {
      interpreter(Set("name", 0, 0.seconds, "rawr"))
      interpreter(Flush("name"))
      interpreter(Get("name", collection.Set.empty)) mustEqual Values(Seq.empty)
    }

    "flushAll" in {
      interpreter(Set("name", 0, 0.seconds, "rawr"))
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