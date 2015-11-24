package com.twitter.finagle.netty4.transport

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.util.{Throw, Return, Await, Future}
import io.netty.channel.{ChannelPromise, ChannelHandlerContext, ChannelOutboundHandlerAdapter}
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class ChannelTransportTest extends FunSuite
  with GeneratorDrivenPropertyChecks with OneInstancePerTest {

  val timeout = 10.seconds

  val (transport, channel) = {
    val ch = new EmbeddedChannel()
    val tr = new ChannelTransport[String, String](ch)
    // We have to remove EmbedChannels' LastInboundHandler to make sure inbound messages
    // reach our own ChannelTransport.
    ch.pipeline().removeFirst()

    (tr, ch)
  }

  def assertSeenWhatsWritten[A](written: Boolean, a: A, seen: Future[A]): Unit =
    assert(!written || (written && Await.result(seen, timeout) == a))

  def assertFailedRead[A](seen: Future[A], e: Exception): Unit = {
    val thrown = intercept[Exception](Await.result(seen, timeout))
    assert(thrown == ChannelException(e, transport.remoteAddress))
    assert(transport.status == Status.Closed)
  }

  test("channel.write before transport.read") {
    forAll { ss: Seq[String] =>
      val written = ss.map(s => channel.writeInbound(s))
      written.zip(ss).foreach { case (w, s) =>
        assertSeenWhatsWritten(w, s, transport.read())
        assert(transport.status == Status.Open)
      }
    }

    val e = new Exception
    channel.pipeline.fireExceptionCaught(e)
    assertFailedRead(transport.read(), e)
  }

  test("transport.read before channel.write") {
    forAll { ss: Seq[String] =>
      val seen = ss.map(_ => transport.read())
      val written = ss.map(s => channel.writeInbound(s))

      written.zip(ss).zip(seen).foreach { case ((w, s), f) =>
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

  test("write (failure)") {
    val e = new Exception()
    channel.pipeline.addLast(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
        // we fail every single write to the pipeline
        promise.setFailure(e)
      }
    })

    forAll { s: String =>
      assert(transport.write(s).poll == Some(Throw(ChannelException(e, transport.remoteAddress))))
    }
  }

  test("write (ok)") {
    channel.pipeline.addLast(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
        // we succeed every single write to the pipeline
        promise.setSuccess()
      }
    })

    forAll { s: String =>
      assert(transport.write(s).poll == Some(Return.Unit))
    }
  }

  test("write (interrupted by caller)") {
    var p: Option[ChannelPromise] = None
    channel.pipeline.addLast(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
        // we store pending promise to make sure it's canceled
        p = Some(promise)
      }
    })

    forAll { s: String =>
      val written = transport.write(s)
      assert(!written.isDefined)

      written.raise(new Exception)
      assert(p.forall(_.isCancelled))
    }
  }

  test("write (canceled by callee)") {
    channel.pipeline.addLast(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit = {
        // we cancel every single write
        promise.cancel(false /*mayInterruptIfRunning*/)
      }
    })

    forAll { s: String =>
      val thrown = intercept[Exception](Await.result(transport.write(s), timeout))
      assert(thrown.isInstanceOf[CancelledWriteException])
    }
  }

  test("write (dropped by callee)") {
    channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false)
    assert(!channel.isWritable)
    assert(transport.status == Status.Busy)

    forAll { s: String =>
      val thrown = intercept[Exception](Await.result(transport.write(s), timeout))
      assert(thrown.isInstanceOf[DroppedWriteException])
    }
  }

  test("read (interrupted by caller)") {
    val e = new Exception
    val seen = transport.read()
    assert(!seen.isDefined)

    seen.raise(e)
    assert(Await.result(transport.onClose, timeout) == e)
    assert(transport.status == Status.Closed)
  }

  test("onClose") {
    val e = new Exception
    assert(!transport.onClose.isDefined)
    channel.pipeline.fireExceptionCaught(e)

    assert(Await.result(transport.onClose, timeout) == ChannelException(e, transport.remoteAddress))
    assert(transport.status == Status.Closed)
  }

  test("close") {
    assert(!transport.onClose.isDefined)

    Await.ready(transport.close(), timeout)
    intercept[ChannelClosedException](throw Await.result(transport.onClose, timeout))
    assert(transport.status == Status.Closed)
    assert(!channel.isOpen)
  }
}
