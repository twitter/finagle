package com.twitter.finagle.netty3.transport

import com.twitter.finagle.ChannelException
import com.twitter.util.{Return, Throw, Await}
import java.net.SocketAddress
import org.jboss.netty.channel._
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.Mockito.{never, times, verify}
import org.mockito.stubbing.OngoingStubbing
import org.mockito.{Matchers, ArgumentCaptor}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{OneInstancePerTest, FunSuite}
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class ChannelTransportTest
  extends FunSuite
  with MockitoSugar
  with OneInstancePerTest {

  // For some reason, the scala compiler has a difficult time with
  // mockito's vararg-v-singlearg 'thenReturns'. We force the
  // selection here by using reflection. Sad.
  def when[T](o: T) =
    Mockito.when(o).asInstanceOf[ {def thenReturn[T](s: T): OngoingStubbing[T]}]

  val ch = mock[Channel]
  val closeFuture = mock[ChannelFuture]
  when(ch.getCloseFuture).thenReturn(closeFuture)
  val remoteAddress = mock[SocketAddress]
  when(ch.getRemoteAddress).thenReturn(remoteAddress)
  when(ch.isReadable).thenReturn(true)
  when(ch.isOpen).thenReturn(true)
  val pipeline = new DefaultChannelPipeline
  val sink = mock[ChannelSink]
  when(ch.getPipeline).thenReturn(pipeline)
  pipeline.attach(ch, sink)
  val trans = new ChannelTransport[String, String](ch)

  def sendUpstream(e: ChannelEvent) {
    val handler = pipeline.getLast.asInstanceOf[ChannelUpstreamHandler]
    val ctx = mock[ChannelHandlerContext]
    handler.handleUpstream(ctx, e)
  }

  def sendUpstreamMessage[T <: Object](msg: T) = sendUpstream({
    val e = mock[MessageEvent]
    when(e.getMessage).thenReturn(msg)
    e
  })

  def newProxyCtx() = new {
    val f = trans.write("one")
    assert(!f.isDefined)
    val captor = ArgumentCaptor.forClass(classOf[ChannelEvent])
    verify(sink, times(1)).eventSunk(Matchers.eq(pipeline), captor.capture)
    assert(captor.getValue.getClass == classOf[DownstreamMessageEvent])
    val dsme = captor.getValue.asInstanceOf[DownstreamMessageEvent]
    assert(dsme.getMessage == "one")
  }

  test("write to the underlying channel, proxying the underlying ChannelFuture (ok)") {
    val ctx = newProxyCtx()
    import ctx._

    dsme.getFuture.setSuccess()
    assert(Await.result(f) == ())
  }

  test("write to the underlying channel, proxying the underlying ChannelFuture (err)") {
    val ctx = newProxyCtx()
    import ctx._

    val exc = new Exception("wtf")
    dsme.getFuture.setFailure(exc)
    val exc1 = intercept[ChannelException] { Await.result(f) }
    assert(exc1 == ChannelException(exc, remoteAddress))
  }

  test("service reads before read())") {
    sendUpstreamMessage("a reply!")
    assert(Await.result(trans.read()) == "a reply!")
  }

  test("service reads after read()") {
    val f = trans.read()
    assert(!f.isDefined)
    sendUpstreamMessage("a reply!")
    assert(Await.result(f) == "a reply!")
  }

  test("closes on interrupted read") {
    val f = trans.read()
    assert(!f.isDefined)
    val closep = trans.onClose
    assert(!closep.isDefined)
    f.raise(new Exception("cancel"))
    assert(closep.isDefined)
  }

  test("maintains interestops") {
    // Not yet connected.
    verify(ch, never).setReadable(Matchers.any[Boolean])

    sendUpstream({
      val e = mock[ChannelStateEvent]
      when(e.getState).thenReturn(ChannelState.CONNECTED)
      when(e.getValue).thenReturn(java.lang.Boolean.TRUE)
      e
    })

    // Initially don't do anything: we buffer one item.
    verify(ch, never).setReadable(Matchers.any[Boolean])

    val f = for {
      a <- trans.read()
      b <- trans.read()
    } yield Seq(a, b)
    verify(ch, never).setReadable(Matchers.any[Boolean])

    // Now we need to make sure that, if we have successive
    // reads, we don't modify interestops again.
    sendUpstreamMessage("1")
    verify(ch, never).setReadable(Matchers.any[Boolean])

    // Send another reply
    sendUpstreamMessage("2")
    verify(ch, never).setReadable(Matchers.any[Boolean])

    // Consumed "1", "2"
    assert(Await.result(f) == Seq("1", "2"))

    // Satisfy the buffering requirement
    sendUpstreamMessage("3")
    verify(ch, times(1)).setReadable(false)
    when(ch.isReadable).thenReturn(false)

    // Now, when messages are queued, make sure we don't change
    // interestops on read.
    sendUpstreamMessage("4")
    sendUpstreamMessage("5")

    assert(Await.result(trans.read()) == "3")
    verify(ch, times(1)).setReadable(false)
    verify(ch, never).setReadable(true)
    assert(Await.result(trans.read()) == "4")
    verify(ch, times(1)).setReadable(false)
    verify(ch, never).setReadable(true)

    // At this point, we have buffered 1 message,
    // and we're not readable. If we attempt to read
    // another one, we become readable again.
    assert(Await.result(trans.read()) == "5")
    verify(ch, times(1)).setReadable(false)
    verify(ch, times(1)).setReadable(true)
    when(ch.isReadable).thenReturn(true)

    // And finally, we don't attemp to change
    // readability again when we enqueue further
    // reads.
    assert(!trans.read().isDefined)
    verify(ch, times(1)).setReadable(false)
    verify(ch, times(1)).setReadable(true)
  }

  test("FIFO queue messages") {
    for (i <- 0 until 10)
      sendUpstreamMessage("message:%d".format(i))

    for (i <- 0 until 10)
      assert(Await.result(trans.read()) == "message:%d".format(i))

    assert(!trans.read().isDefined)
  }

  val exc = new Exception("sad panda")

  test("handle exceptions on subsequent ops") {
    sendUpstream({
      val e = mock[ExceptionEvent]
      when(e.getCause).thenReturn(exc)
      e
    })

    val exc1 = intercept[ChannelException] { Await.result(trans.read()) }
    assert(exc1 == ChannelException(exc, remoteAddress))
  }

  test("handle exceptions on pending reads") {  // writes are taken care of by netty
    val f = trans.read()
    assert(!f.isDefined)
    sendUpstream({
      val e = mock[ExceptionEvent]
      when(e.getCause).thenReturn(exc)
      e
    })

    val exc1 = intercept[ChannelException] { Await.result(f) }
    assert(exc1 == ChannelException(exc, remoteAddress))
  }

  test("satisfy onClose") {
    assert(!trans.onClose.isDefined)
    val exc = new Exception("close exception")
    sendUpstream({
      val e = mock[ExceptionEvent]
      when(e.getCause).thenReturn(exc)
      e
    })

    assert(Await.result(trans.onClose) ==
      ChannelException(exc, remoteAddress))
  }

  test("satisfy pending reads") {
    val f = trans.read()
    assert(!f.isDefined)

    sendUpstreamMessage("a")
    sendUpstreamMessage("b")
    val exc = new Exception("nope")
    sendUpstream({
      val e = mock[ExceptionEvent]
      when(e.getCause).thenReturn(exc)
      e
    })

    assert(f.poll == Some(Return("a")))
    assert(trans.read().poll == Some(Return("b")))
    assert(trans.read().poll == Some(Throw(ChannelException(exc, remoteAddress))))
  }
}
