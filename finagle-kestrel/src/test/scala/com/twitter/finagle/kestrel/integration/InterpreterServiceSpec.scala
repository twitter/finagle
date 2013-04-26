package com.twitter.finagle.kestrel.integration

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Server
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.{Await, Time}
import java.net.InetSocketAddress
import org.specs.SpecificationWithJUnit

class InterpreterServiceSpec extends SpecificationWithJUnit {
  "InterpreterService" should {
    var server: Server = null
    var client: Service[Command, Response] = null
    var address: InetSocketAddress = null

    doBefore {
      server = new Server(new InetSocketAddress(0))
      address = server.start().localAddress.asInstanceOf[InetSocketAddress]
      client = ClientBuilder()
        .hosts("localhost:" + address.getPort)
        .codec(Kestrel())
        .hostConnectionLimit(1)
        .build()
    }

    doAfter {
      server.stop()
    }

    val queueName   = "name"
    val value = "value"

    "set & get" in {
      val result = for {
        _ <- client(Flush(queueName))
        _ <- client(Set(queueName, Time.now, value))
        r <- client(Get(queueName))
      } yield r
      Await.result(result, 1.second) mustEqual Values(Seq(Value(queueName, value)))
    }

    "transactions" in {
      "set & get/open & get/abort" in {
        val result = for {
          _ <- client(Set(queueName, Time.now, value))
          _ <- client(Open(queueName))
          _ <- client(Abort(queueName))
          r <- client(Open(queueName))
        } yield r
        Await.result(result, 1.second) mustEqual Values(Seq(Value(queueName, value)))
      }
    }
  }
}
