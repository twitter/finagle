package com.twitter.finagle.kestrel.integration

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Server
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import java.net.{InetSocketAddress, InetAddress}
import com.twitter.util.{Await, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class InterpreterServiceTest extends FunSuite {
  val queueName = "name"
  val value = "value"

  def exec(fn: (Server, Service[Command, Response]) => Unit) {
    val server: Server = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val address: InetSocketAddress = server.start().localAddress.asInstanceOf[InetSocketAddress]
    val client: Service[Command, Response] = ClientBuilder()
      .hosts(address)
      .codec(Kestrel())
      .hostConnectionLimit(1)
      .build()

    fn(server, client)

    server.stop()
  }

  test("InterpreterService should set & get") {
    exec { case (server, client) =>
      val result = for {
        _ <- client(Flush(queueName))
        _ <- client(Set(queueName, Time.now, value))
        r <- client(Get(queueName))
      } yield r
      assert(Await.result(result, 1.second) === Values(Seq(Value(queueName, value))))
    }
  }

  test("InterpreterService: transactions should set & get/open & get/abort") {
    exec { case (server, client) =>
      val result = for {
        _ <- client(Set(queueName, Time.now, value))
        _ <- client(Open(queueName))
        _ <- client(Abort(queueName))
        r <- client(Open(queueName))
      } yield r
      assert(Await.result(result, 1.second) === Values(Seq(Value(queueName, value))))
    }
  }
}
