package com.twitter.finagle.kestrel.integration

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.Service
import com.twitter.finagle.kestrel.Server
import org.specs.Specification
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.{Time, RandomSocket}
import com.twitter.conversions.time._

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
        .codec(new Kestrel)
        .build()
    }

    doAfter {
      server.stop()
    }

    "set & get" in {
      val queueName   = "name"
      val value = "value"
      val result = for {
        _ <- client(Flush(queueName))
        _ <- client(Set(queueName, Time.now, value))
        r <- client(Get(queueName, collection.Set.empty))
      } yield r
      result(1.second) mustEqual Values(Seq(Value(queueName, value)))
    }
  }
}