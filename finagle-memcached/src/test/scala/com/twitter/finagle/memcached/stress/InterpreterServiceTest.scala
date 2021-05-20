package com.twitter.finagle.memcached.stress

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Address
import com.twitter.finagle.Memcached
import com.twitter.finagle.Name
import com.twitter.finagle.Service
import com.twitter.finagle.memcached.integration.external.InProcessMemcached
import com.twitter.finagle.memcached.protocol._
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Time}
import java.net.{InetAddress, InetSocketAddress}
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite

class InterpreterServiceTest extends AnyFunSuite with BeforeAndAfter {

  val TimeOut = 15.seconds

  private def awaitResult[T](awaitable: Awaitable[T]): T = Await.result(awaitable, TimeOut)

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
    val _key = "key"
    val value = Buf.Utf8("value")
    val zero = Buf.Utf8("0")
    val start = System.currentTimeMillis
    (0 until 100) map { i =>
      val key = _key + i
      awaitResult(client(Delete(Buf.Utf8(key))))
      awaitResult(client(Set(Buf.Utf8(key), 0, Time.epoch, value)))
      assert(
        awaitResult(client(Get(Seq(Buf.Utf8(key))))) == Values(
          Seq(Value(Buf.Utf8(key), value, None, Some(zero)))
        )
      )
    }
    val end = System.currentTimeMillis
    // println("%d ms".format(end - start))
  }

}
