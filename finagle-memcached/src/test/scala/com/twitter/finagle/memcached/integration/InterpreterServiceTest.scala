package com.twitter.finagle.memcached.integration

import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.protocol.text.Memcached
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.io.Buf
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Time}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class InterpreterServiceTest extends FunSuite with BeforeAndAfter {

  var server: InProcessMemcached = null
  var client: Service[Command, Response] = null

  before {
    server = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val address = server.start().boundAddress
    client = ClientBuilder()
      .hosts(address)
      .codec(new Memcached)
      .hostConnectionLimit(1)
      .build()
  }

  after {
    server.stop()
  }

  test("set & get") {
    val key = Buf.Utf8("key")
    val value = Buf.Utf8("value")
    val zero = "0"
    val result = for {
      _ <- client(Delete(key))
      _ <- client(Set(key, 0, Time.epoch, value))
      r <- client(Get(Seq(key)))
    } yield r
    assert(Await.result(result, 1.second) == Values(Seq(Value(key, value, None, Some(Buf.Utf8(zero))))))
    assert(client.isAvailable)
  }

  test("quit") {
    val result = client(Quit())
    assert(Await.result(result) == NoOp())
  }

}
