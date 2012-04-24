package com.twitter.finagle.memcached.stress

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.Server
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.Service
import com.twitter.util.Time
import java.net.InetSocketAddress
import org.specs.SpecificationWithJUnit

class InterpreterServiceSpec extends SpecificationWithJUnit {
  "InterpreterService" should {
    var server: Server = null
    var client: Service[Command, Response] = null

    doBefore {
      server = new Server(new InetSocketAddress(0))
      val address = server.start().localAddress
      client = ClientBuilder()
        .hosts(address)
        .codec(new Memcached)
        .hostConnectionLimit(1)
        .build()
    }

    doAfter {
      server.stop()
    }

    "set & get" in {
      val _key   = "key"
      val value = "value"
      val zero = "0"
      val start = System.currentTimeMillis
      (0 until 100) map { i =>
        val key = _key + i
        client(Delete(key))()
        client(Set(key, 0, Time.epoch, value))()
        client(Get(Seq(key)))() mustEqual Values(Seq(Value(key, value, None, Some(zero))))
      }
      val end = System.currentTimeMillis
      // println("%d ms".format(end - start))
    }
  }
}
