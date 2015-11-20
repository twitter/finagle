package com.twitter.finagle.netty4.channel

import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.channel._
import io.netty.util.{AttributeKey, Attribute}
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.mockito.Mockito.when
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ChannelRequestStatsHandlerTest extends FunSuite with MockitoSugar {

  def mkAttr(ai: AtomicInteger): Attribute[AtomicInteger] = new Attribute[AtomicInteger] {
    def set(value: AtomicInteger): Unit = ai.set(value.get())
    def get(): AtomicInteger = ai

    def key(): AttributeKey[AtomicInteger] = ???
    def getAndRemove(): AtomicInteger = ???
    def remove(): Unit = ???
    def compareAndSet(oldValue: AtomicInteger, newValue: AtomicInteger): Boolean = ???
    def setIfAbsent(value: AtomicInteger): AtomicInteger = ???
    def getAndSet(value: AtomicInteger): AtomicInteger = ???
  }

  test("ChannelRequestStatsHandler counts messages") {
    val sr = new InMemoryStatsReceiver()

    def requestsEqual(requests: Seq[Float]) =
      assert(
        sr.stat("connection_requests")() == requests
      )

    val handler = new ChannelRequestStatsHandler(sr)
    requestsEqual(Seq.empty[Float])

    val ctx = mock[ChannelHandlerContext]
    val reqAttr = mkAttr(new AtomicInteger(0))
    when(ctx.attr(ChannelRequestStatsHandler.ConnectionRequestsKey)).thenReturn(reqAttr)


    val msg = new Object

    // first connection sends two messages
    handler.channelActive(ctx)
    handler.channelRead(ctx, msg)
    handler.channelRead(ctx, msg)
    handler.channelInactive(ctx)

    // second connection sends zero
    handler.channelActive(ctx)
    handler.channelInactive(ctx)

    // third connection sends one
    handler.channelActive(ctx)
    handler.channelRead(ctx, msg)
    handler.channelInactive(ctx)

    requestsEqual(Seq(2.0f, 0.0f, 1.0f))
  }
}
