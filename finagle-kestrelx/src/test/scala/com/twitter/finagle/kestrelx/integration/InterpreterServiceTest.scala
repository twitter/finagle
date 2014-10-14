package com.twitter.finagle.kestrelx.integration

import java.net.InetSocketAddress

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrelx.Server
import com.twitter.finagle.kestrelx.protocol._
import com.twitter.io.Buf
import com.twitter.util.{Await, Time}

@RunWith(classOf[JUnitRunner])
class InterpreterServiceTest extends FunSuite {

  trait HelperTrait {
    var server: Server = null
    var client: Service[Command, Response] = null
    var address: InetSocketAddress = null
    val queueName = Buf.Utf8("name")
    val value = Buf.Utf8("value")

    def doBefore() = {
      server = new Server(new InetSocketAddress(0))
      address = server.start().localAddress.asInstanceOf[InetSocketAddress]
      client = ClientBuilder()
        .hosts("localhost:" + address.getPort)
        .codec(Kestrel())
        .hostConnectionLimit(1)
        .build()
    }

    def doAfter() = { server.stop() }
  }

  if (!sys.props.contains("SKIP_FLAKY"))
  test("InterpreterService should set & get") {
    new HelperTrait {
      doBefore()

      val result = for {
        _ <- client(Flush(queueName))
        _ <- client(Set(queueName, Time.now, value))
        r <- client(Get(queueName))
      } yield r
      assert(Await.result(result, 1.second) === Values(Seq(Value(queueName, value))))

      doAfter()
    }
  }

  test("InterpreterService: transactions should set & get/open & get/abort") {
    new HelperTrait {
      doBefore()

      val result = for {
        _ <- client(Set(queueName, Time.now, value))
        _ <- client(Open(queueName))
        _ <- client(Abort(queueName))
        r <- client(Open(queueName))
      } yield r
      assert(Await.result(result, 1.second) === Values(Seq(Value(queueName, value))))

      doAfter()
    }
  }
}
