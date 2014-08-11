package com.twitter.finagle.kestrel.integration

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.Server
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.util.{Await, Time}
import java.net.InetSocketAddress

import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class InterpreterServiceTest extends FunSuite with BeforeAndAfter {
  var server: Server = null
  var client: Service[Command, Response] = null
  var address: InetSocketAddress = null

  before {
    server = new Server(new InetSocketAddress(0))
    address = server.start().localAddress.asInstanceOf[InetSocketAddress]
    client = ClientBuilder()
      .hosts("localhost:" + address.getPort)
      .codec(Kestrel())
      .hostConnectionLimit(1)
      .build()
  }

  after {
    server.stop()
  }

  val queueName   = "name"
  val value = "value"

  test("set & get") {
    val result = for {
      _ <- client(Flush(queueName))
      _ <- client(Set(queueName, Time.now, value))
      r <- client(Get(queueName))
    } yield r
    assert(Await.result(result, 1.second) === Values(Seq(Value(queueName, value))))
  }

  test("transactions - set & get/open & get/abort") {
    val result = for {
      _ <- client(Set(queueName, Time.now, value))
      _ <- client(Open(queueName))
      _ <- client(Abort(queueName))
      r <- client(Open(queueName))
    } yield r
    assert(Await.result(result, 1.second) === Values(Seq(Value(queueName, value))))
  }
}
