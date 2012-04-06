package com.twitter.finagle.memcached.unit

import com.twitter.finagle.memcached.Interpreter
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.AtomicMap
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.Time
import org.jboss.netty.buffer.ChannelBuffer
import org.specs.SpecificationWithJUnit
import scala.collection.mutable

class InterpreterSpec extends SpecificationWithJUnit {
  "Interpreter" should {
    val map = mutable.Map[ChannelBuffer, ChannelBuffer]()
    val interpreter = new Interpreter(new AtomicMap(Seq(map)))

    "set & get" in {
      val key   = "foo"
      val value = "bar"
      interpreter(Delete(key))
      interpreter(Set(key, 0, Time.epoch, value))
      interpreter(Get(Seq(key))) mustEqual Values(Seq(Value(key, value)))
    }

    "quit" in {
      interpreter(Quit()) mustEqual NoOp()
    }
  }
}
