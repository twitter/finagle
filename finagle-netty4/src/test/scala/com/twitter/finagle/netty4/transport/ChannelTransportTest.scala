package com.twitter.finagle.netty4.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Future, Return, Time, Throw}
import io.netty.channel.{ChannelException => _, _}
import io.netty.channel.embedded.EmbeddedChannel
import org.scalatest.OneInstancePerTest
import org.scalatest.concurrent.Eventually._
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite

class ChannelTransportTest
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks
    with OneInstancePerTest
    with MockitoSugar {

  val timeout = 10.seconds

  val channel = new EmbeddedChannel()
  val channelTransport = new ChannelTransport(channel)
  val transport = Transport.cast[String, String](channelTransport)

  def assertSeenWhatsWritten[A](written: Boolean, a: A, seen: Future[A]): Unit =
    assert(!written || (written && Await.result(seen, timeout) == a))

  def assertFailedRead[A](seen: Future[A], e: Exception): Unit = {
    val thrown = intercept[Exception](Await.result(seen, timeout))
    assert(thrown == ChannelException(e, transport.context.remoteAddress))
    assert(transport.status == Status.Closed)
  }

  test("ChannelTransport status is open when not failed and channel is not closed") {
    assert(channel.isOpen)
    assert(transport.status == Status.Open)
  }

  test("ChannelTransport status is closed when failed") {
    channelTransport.failed.compareAndSet(false, true)
    assert(transport.status == Status.Closed)
  }

  test("ChannelTransport status is closed when channel is closed") {
    channel.close()
    assert(transport.status == Status.Closed)
  }

  test("ChannelTransport still works if we channel.write before transport.read") {
    forAll { ss: Seq[String] =>
      val written = ss.map(s => channel.writeInbound(s))
      written.zip(ss).foreach {
        case (w, s) =>
          assertSeenWhatsWritten(w, s, transport.read())
          assert(transport.status == Status.Open)
      }
    }

    val e = new Exception
    channel.pipeline.fireExceptionCaught(e)
    assertFailedRead(transport.read(), e)
  }

  test("ChannelTransport still works if we transport.read before channel.write") {
    forAll { ss: Seq[String] =>
      val seen = ss.map(_ => transport.read())
      val written = ss.map(s => channel.writeInbound(s))

      written.zip(ss).zip(seen).foreach {
        case ((w, s), f) =>
          assertSeenWhatsWritten(w, s, f)
          assert(transport.status == Status.Open)
      }
    }

    val e = new Exception
    val seen = transport.read()
    assert(!seen.isDefined)
    channel.pipeline.fireExceptionCaught(e)
    assertFailedRead(seen, e)
  }

  test("ChannelTransport propagates failures back up on write") {
    val e = new Exception()
    channel.pipeline.addLast(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
        // we fail every single write to the pipeline
        promise.setFailure(e)
      }
    })

    forAll { s: String =>
      assert(
        transport.write(s).poll == Some(
          Throw(ChannelException(e, transport.context.remoteAddress))))
    }
  }

  test("ChannelTransport writes successfully") {
    forAll { s: String =>
      assert(transport.write(s).map(_ => channel.readOutbound[String]).poll == Some(Return(s)))
    }
  }

  test("ChannelTransport fails the connection when the read is interrupted") {
    val e = new Exception
    val seen = transport.read()
    assert(!seen.isDefined)

    seen.raise(e)
    assert(Await.result(transport.onClose, timeout) == e)
    assert(transport.status == Status.Closed)
    assert(!channel.isOpen)
    assert(!channel.isActive)
  }

  test("ChannelTransport satisfies onClose when it closes") {
    val e = new Exception
    assert(!transport.onClose.isDefined)
    channel.pipeline.fireExceptionCaught(e)

    assert(
      Await
        .result(transport.onClose, timeout) == ChannelException(e, transport.context.remoteAddress))
    assert(transport.status == Status.Closed)
  }

  test("ChannelTransport cuts the connection on close") {
    assert(!transport.onClose.isDefined)

    Await.ready(transport.close(), timeout)
    intercept[ChannelClosedException] {
      throw Await.result(transport.onClose, timeout)
    }
    assert(transport.status == Status.Closed)
    assert(!channel.isOpen)
    assert(!channel.isActive)
  }

  test("ChannelTransport closes when the remote closes") {
    channel.close()
    assert(!channel.isOpen)
    assert(!channel.isActive)

    // this is subtle.  transport.onClose returns a Future[Throwable].  we want
    // to ensure that we expect a ChannelClosedException, but it should be from
    // a successful Future, not a failed one.
    val Return(t) = Await.result(transport.onClose.liftToTry, timeout)
    intercept[ChannelClosedException] {
      throw t
    }
    assert(transport.status == Status.Closed)
  }

  test("ChannelTransport drains the offer queue before reading from the channel") {
    val channel = spy(new EmbeddedChannel())
    channel.config().setAutoRead(false)

    val trans = Transport.cast[String, String](new ChannelTransport(channel))

    // buffer data in the underlying channel
    channel.writeInbound("one")
    channel.writeInbound("two")
    channel.writeInbound("three")
    assert("one" == Await.result(trans.read(), timeout))
    assert("two" == Await.result(trans.read(), timeout))

    // no reads from the channel yet
    verify(channel, never).read()

    assert("three" == Await.result(trans.read(), timeout))

    // an empty queue leads to a single read
    verify(channel, times(1)).read()

    // the offer q is drained, so reading another message triggers another channel read
    trans.read()
    eventually { verify(channel, times(2)).read() }
  }

  test("ChannelTransport buffers a single read when backpressure is enabled") {
    val message = "message 1"

    val channel = spy(new EmbeddedChannel())
    channel.config().setAutoRead(false)

    val trans = Transport.cast[String, String](new ChannelTransport(channel))

    // On startup, the ChannelTransport should queue one read
    channel.pipeline().fireChannelActive()
    verify(channel, times(1)).read()

    // When we get a message, we shouldn't do another read until the first message is taken
    channel.writeInbound(message)
    eventually { verify(channel, times(1)).read() }

    // We take the message that was queued, so we should have attempted
    // the next read
    assert(Await.result(trans.read(), timeout) == message)
    eventually { verify(channel, times(2)).read() }
  }

  test("ChannelTransport will continue to trigger reads as waiting reads are fulfilled") {
    val channel = spy(new EmbeddedChannel())
    channel.config().setAutoRead(false)

    val trans = Transport.cast[String, String](new ChannelTransport(channel))

    verify(channel, never).read()
    // buffer data in the underlying channel
    val readOne = trans.read()
    val readTwo = trans.read()
    val readThree = trans.read()

    verify(channel, times(3)).read()

    channel.writeInbound("one")
    // Gets called twice, once by `channelRead0` and once by `channelReadComplete`
    verify(channel, times(4)).read()
    assert("one" == Await.result(readOne, timeout))

    channel.writeInbound("two")
    // Called twice for the same reason
    verify(channel, times(5)).read()
    assert("two" == Await.result(readTwo, timeout))

    channel.writeInbound("three")
    // Called twice to buffer one inbound message to attempt to detect close events
    verify(channel, times(6)).read()
    assert("three" == Await.result(readThree, timeout))
  }

  test("buffered messages are not flushed on transport shutdown") {
    val em = new EmbeddedChannel
    val ct = Transport.cast[String, String](new ChannelTransport(em))
    em.writeInbound("one")
    Await.ready(ct.close())
    assert(Await.result(ct.read(), 1.second) == "one")
  }

  test("buffered messages are not flushed on exceptions") {
    val em = new EmbeddedChannel
    val ct = Transport.cast[String, String](new ChannelTransport(em))
    // buffer a message
    em.writeInbound("one")

    // channel failure -> transport is failed
    em.pipeline().fireExceptionCaught(new Exception("boom"))
    assert(ct.status == Status.Closed)

    assert(Await.result(ct.read(), 1.second) == "one")
  }

  test("pending transport reads are failed on channel close") {
    val em = new EmbeddedChannel
    val ct = Transport.cast[String, String](new ChannelTransport(em))
    val read = ct.read()
    Await.ready(ct.close(), 1.second)
    intercept[ChannelClosedException] { Await.result(read, 1.second) }
  }

  test("disabling autoread midstream is safe") {
    val em = new EmbeddedChannel
    em.config.setAutoRead(true)
    val ct = new ChannelTransport(em)
    val transport = Transport.cast[String, String](ct)
    val f = ct.read()
    em.config.setAutoRead(false)

    em.writeInbound("one")

    assert(ct.ReadManager.getMsgsNeeded == 0)
  }

  test("offer failures fail the transport") {
    val em = new EmbeddedChannel
    val ct = new ChannelTransport(em, new AsyncQueue[Any](maxPendingOffers = 1))
    val transport = Transport.cast[String, String](ct)

    // full read queue
    em.writeInbound("full")

    // rejected
    em.writeInbound("doomed")

    assert(ct.status == Status.Closed)
  }

  test("calling close multiple times only closes the channel once") {
    val ch = new EmbeddedChannel()
    val transport = new ChannelTransport(ch)
    assert(!transport.closed.isDefined)
    transport.close(Time.now)
    assert(!ch.isOpen)
    assert(transport.closed.isDefined)
    transport.close(Time.now)
    transport.close(Time.now)
    assert(transport.closed.isDefined)
    // Nothing bad happened
    succeed
  }
}
