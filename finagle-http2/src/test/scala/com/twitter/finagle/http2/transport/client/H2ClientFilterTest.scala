package com.twitter.finagle.http2.transport.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.util.{Await, Awaitable}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http2.{DefaultHttp2PingFrame, Http2PingFrame}
import org.scalatest.funsuite.AnyFunSuite

class H2ClientFilterTest extends AnyFunSuite {

  private[this] def await[T](t: Awaitable[T]): T =
    Await.result(t, 15.seconds)

  abstract class Ctx {
    // We need to disable the default FailureDetector config, otherwise it will trigger extra
    // pings during the test cases
    def params: Stack.Params =
      Stack.Params.empty + FailureDetector.Param(FailureDetector.NullConfig)

    val pingHandler: H2ClientFilter = new H2ClientFilter(params)
    val testChannel: EmbeddedChannel = new EmbeddedChannel(pingHandler)
  }

  test("Succeeds when Ping is acked") {
    new Ctx {
      val pingF = pingHandler.ping()
      testChannel.flushOutbound()

      assert(!pingF.isDefined)
      assert(testChannel.readOutbound[Http2PingFrame]() == H2ClientFilter.Ping)
      assert(!pingF.isDefined)
      assert(!testChannel.writeInbound(new DefaultHttp2PingFrame(0L, true)))
      await(pingF)
    }
  }

  test("Fails with outstanding pings") {
    new Ctx {
      val legalPing = pingHandler.ping()
      val illegalPing = pingHandler.ping()

      testChannel.flushOutbound()

      intercept[H2ClientFilter.PingOutstandingFailure] {
        await(illegalPing)
      }
    }
  }

}
