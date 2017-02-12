package com.twitter.finagle.netty4.channel

import io.netty.channel.ChannelOutboundHandlerAdapter
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class ConnectPromiseDelayListenersTest extends FunSuite {

  class Ctx extends ChannelOutboundHandlerAdapter
      with BufferingChannelOutboundHandler
      with ConnectPromiseDelayListeners {

    val channel = new EmbeddedChannel(this)

    val p1 = channel.newPromise()
    val p2 = channel.newPromise()
    val ctx = channel.pipeline().context(this)
  }

  test("cancel when cancelled (success)") {
    new Ctx {
      p1.addListener(proxyCancellationsTo(p2, ctx))
      p1.cancel(false)
      assert(p2.isCancelled)
    }
  }

  test("cancel when cancelled (already succeed)") {
    new Ctx {
      p1.addListener(proxyCancellationsTo(p2, ctx))
      p2.setSuccess()
      p1.cancel(false)
      assert(!channel.isOpen)
    }
  }

  test("fail when failed") {
    new Ctx {
      val e = new Exception("not good")
      p1.addListener(proxyFailuresTo(p2))
      p1.setFailure(e)
      assert(p2.cause() == e)
    }
  }
}
