package com.twitter.finagle.memcached.integration

import com.twitter.finagle.Address
import com.twitter.finagle.Memcached
import com.twitter.finagle.Name
import com.twitter.finagle.Service
import com.twitter.finagle.memcached.Interpreter
import com.twitter.finagle.memcached.protocol._
import com.twitter.io.Buf
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Time}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.{BeforeAndAfter, FunSuite}

class InterpreterServiceTest extends FunSuite with BeforeAndAfter {

  var server: InProcessMemcached = null
  var client: Service[Command, Response] = null

  before {
    server = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val address = Address(server.start().boundAddress.asInstanceOf[InetSocketAddress])
    client = Memcached.client
      .connectionsPerEndpoint(1)
      .newService(Name.bound(address), "memcache")
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
  }

  test("quit") {
    val result = client(Quit())
    assert(Await.result(result) == NoOp())
  }

  test("cas") {
    val key = Buf.Utf8("key")
    val value = Buf.Utf8("value")
    val zero = "0"
    val result = for {
      _ <- client(Delete(key))
      _ <- client(Set(key, 0, Time.epoch, value))
      r <- client(Get(Seq(key)))
    } yield r

    assert(Await.result(result, 1.second) == Values(Seq(Value(key, value, None, Some(Buf.Utf8(zero))))))

    val newValue = Buf.Utf8("new-value")
    // client tries to cas using a wrong checksum and fails to update the value
    val wrongChecksum = Interpreter.generateCasUnique(Buf.Utf8("wrong-value"))
    Await.result(client(Cas(key, 0, Time.epoch + 5.seconds, newValue, wrongChecksum)))
    val retrievedValue = Await.result(client(Get(Seq(key))))
    assert(retrievedValue == Values(Seq(Value(key, value, None, Some(Buf.Utf8(zero))))))

    // client does cas using the right checksum and successfully updates the value
    val rightChecksum = Interpreter.generateCasUnique(value)
    Await.result(client(Cas(key, 0, Time.epoch + 5.seconds, newValue, rightChecksum)))
    val newRetrievedValue = Await.result(client(Get(Seq(key))))
    assert(newRetrievedValue == Values(Seq(Value(key, newValue, None, Some(Buf.Utf8(zero))))))
  }


}
