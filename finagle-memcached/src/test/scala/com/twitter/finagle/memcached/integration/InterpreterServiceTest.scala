package com.twitter.finagle.memcached.integration

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address
import com.twitter.finagle.Memcached
import com.twitter.finagle.Name
import com.twitter.finagle.Service
import com.twitter.finagle.memcached.Interpreter
import com.twitter.finagle.memcached.integration.external.InProcessMemcached
import com.twitter.finagle.memcached.protocol._
import com.twitter.io.Buf
import com.twitter.conversions.DurationOps._
import com.twitter.util.{Await, Awaitable, Time}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class InterpreterServiceTest extends AnyFunSuite with BeforeAndAfter {

  private val TimeOut = 15.seconds

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

  private var server: InProcessMemcached = null
  private var client: Service[Command, Response] = null

  private val baseClient: Memcached.Client = Memcached.client

  before {
    server = new InProcessMemcached(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
    val address = Address(server.start().boundAddress.asInstanceOf[InetSocketAddress])
    client = baseClient
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
    assert(awaitResult(result) == Values(Seq(Value(key, value, None, Some(Buf.Utf8(zero))))))
  }

  test("huge set value to force framing") {
    val key = Buf.Utf8("key")
    val value = Buf.Utf8("value" * 5000)
    val zero = "0"
    val result = for {
      _ <- client(Delete(key))
      _ <- client(Set(key, 0, Time.epoch, value))
      r <- client(Get(Seq(key)))
    } yield r
    assert(awaitResult(result) == Values(Seq(Value(key, value, None, Some(Buf.Utf8(zero))))))
  }

  test("quit") {
    val result = client(Quit())
    assert(awaitResult(result) == NoOp)
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

    assert(awaitResult(result) == Values(Seq(Value(key, value, None, Some(Buf.Utf8(zero))))))

    val newValue = Buf.Utf8("new-value")
    // client tries to cas using a wrong checksum and fails to update the value
    val wrongChecksum = Interpreter.generateCasUnique(Buf.Utf8("wrong-value"))
    awaitResult(client(Cas(key, 0, Time.epoch + 5.seconds, newValue, wrongChecksum)))
    val retrievedValue = awaitResult(client(Get(Seq(key))))
    assert(retrievedValue == Values(Seq(Value(key, value, None, Some(Buf.Utf8(zero))))))

    // client does cas using the right checksum and successfully updates the value
    val rightChecksum = Interpreter.generateCasUnique(value)
    awaitResult(client(Cas(key, 0, Time.epoch + 5.seconds, newValue, rightChecksum)))
    val newRetrievedValue = awaitResult(client(Get(Seq(key))))
    assert(newRetrievedValue == Values(Seq(Value(key, newValue, None, Some(Buf.Utf8(zero))))))
  }
}
