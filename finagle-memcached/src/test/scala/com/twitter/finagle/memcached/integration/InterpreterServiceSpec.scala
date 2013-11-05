package com.twitter.finagle.memcached.integration

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Time}
import java.net.InetSocketAddress
import org.specs.SpecificationWithJUnit

class InterpreterServiceSpec extends SpecificationWithJUnit {
  "InterpreterService" should {
    var server: InProcessMemcached = null
    var client: Service[Command, Response] = null

    doBefore {
      server = new InProcessMemcached(new InetSocketAddress(0))
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
      val key   = "key"
      val value = "value"
      val zero = "0"
      val result = for {
        _ <- client(Delete(key))
        _ <- client(Set(key, 0, Time.epoch, value))
        r <- client(Get(Seq(key)))
      } yield r
      Await.result(result, 1.second) mustEqual Values(Seq(Value(key, value, None, Some(zero))))
      client.isAvailable must beTrue
    }

    "quit" in {
      val result = client(Quit())
      Await.result(result) mustEqual NoOp()
    }
  }
}
