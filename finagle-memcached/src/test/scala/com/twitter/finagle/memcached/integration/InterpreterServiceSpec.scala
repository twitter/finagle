package com.twitter.finagle.memcached.integration

import org.specs.Specification
import com.twitter.finagle.memcached.Server
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.util.TimeConversions._
import com.twitter.finagle.Service
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.{Time, RandomSocket}

object InterpreterServiceSpec extends Specification {
  "InterpreterService" should {
    var server: Server = null
    var client: Service[Command, Response] = null

    doBefore {
      val address = RandomSocket()
      server = new Server(address)
      server.start()
      client = ClientBuilder()
        .hosts("localhost:" + address.getPort)
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
      result(1.second) mustEqual Values(Seq(Value(key, value, None, Some(zero))))
      client.isAvailable must beTrue
    }

    "quit" in {
      val result = client(Quit())
      result() mustEqual NoOp()
    }

  }
}
