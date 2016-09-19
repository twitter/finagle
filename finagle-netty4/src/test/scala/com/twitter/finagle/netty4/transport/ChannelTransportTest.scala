package com.twitter.finagle.netty4.transport

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.util.{Throw, Return, Await, Future}
import io.netty.channel.{ChannelException => _, _}
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.ssl.SslHandler
import org.junit.runner.RunWith
import org.scalatest.{OneInstancePerTest, FunSuite}
import org.scalatest.concurrent.Eventually._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.prop.GeneratorDrivenPropertyChecks
import org.mockito.Mockito._
import java.security.cert.Certificate
import javax.net.ssl.{SSLSession, SSLEngine}

@RunWith(classOf[JUnitRunner])
class ChannelTransportTest extends FunSuite
  with GeneratorDrivenPropertyChecks with OneInstancePerTest with MockitoSugar {

  val timeout = 10.seconds

  val (transport, channel) = {
    val ch = new EmbeddedChannel()
    val tr = new ChannelTransport[String, String](ch)
    (tr, ch)
  }

  def assertSeenWhatsWritten[A](written: Boolean, a: A, seen: Future[A]): Unit =
    assert(!written || (written && Await.result(seen, timeout) == a))

  def assertFailedRead[A](seen: Future[A], e: Exception): Unit = {
    val thrown = intercept[Exception](Await.result(seen, timeout))
    assert(thrown == ChannelException(e, transport.remoteAddress))
    assert(transport.status == Status.Closed)
  }

  test("ChannelTransport still works if we channel.write before transport.read") {
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

  test("ChannelTransport still works if we transport.read before channel.write") {
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

  test("ChannelTransport propagates failures back up on write") {
    val e = new Exception()
    channel.pipeline.addLast(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
        // we fail every single write to the pipeline
        promise.setFailure(e)
      }
    })

    forAll { s: String =>
      assert(transport.write(s).poll == Some(Throw(ChannelException(e, transport.remoteAddress))))
    }
  }

  test("ChannelTransport writes successfully") {
    forAll { s: String =>
      assert(transport.write(s).map(_ => channel.readOutbound[String]).poll == Some(Return(s)))
    }
  }

  test("ChannelTransport cancels the underlying write when interrupted by the caller") {
    var p: Option[ChannelPromise] = None
    channel.pipeline.addLast(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
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

  test("ChannelTransport write propagates a failure back when it's cancelled in the netty layer") {
    channel.pipeline.addLast(new ChannelOutboundHandlerAdapter {
      override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise): Unit = {
        // we cancel every single write
        promise.cancel(false /*mayInterruptIfRunning*/)
      }
    })

    forAll { s: String =>
      val thrown = intercept[Exception](Await.result(transport.write(s), timeout))
      // we do this because forAll doesn't seem to work with just intercepts
      assert(thrown.isInstanceOf[CancelledWriteException])
    }
  }

  test("ChannelTransport throws properly on writes when the other end hangs up") {
    channel.unsafe().outboundBuffer().setUserDefinedWritability(1, false)
    assert(!channel.isWritable)
    assert(transport.status == Status.Busy)

    forAll { s: String =>
      val e = intercept[Exception](Await.result(transport.write(s), timeout))
      // we do this because forAll doesn't seem to work with just intercepts
      assert(e.isInstanceOf[DroppedWriteException])
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

    assert(Await.result(transport.onClose, timeout) == ChannelException(e, transport.remoteAddress))
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
    val Return(t) = Await.result(transport.onClose.liftToTry, 5.seconds)
    intercept[ChannelClosedException] {
      throw t
    }
    assert(transport.status == Status.Closed)
  }

  test("peerCertificate") {
    val engine = mock[SSLEngine]
    val session = mock[SSLSession]
    val cert = mock[Certificate]
    when(engine.getSession).thenReturn(session)
    when(session.getPeerCertificates).thenReturn(Array(cert))
    val ch = new EmbeddedChannel(new SslHandler(engine))
    val tr = new ChannelTransport[String, String](ch)

    assert(tr.peerCertificate == Some(cert))
  }


  test("ChannelTransport drains the offer queue before reading from the channel") {
    val channel = spy(new EmbeddedChannel())

    val noAuto = new DefaultChannelConfig(channel)
    noAuto.setAutoRead(false)
    when(channel.config()).thenReturn(noAuto)

    val trans = new ChannelTransport[String, String](channel)

    // buffer data in the underlying channel
    channel.writeInbound("one")
    channel.writeInbound("two")
    channel.writeInbound("three")
    assert("one" == Await.result(trans.read(), 5.seconds))
    assert("two" == Await.result(trans.read(), 5.seconds))
    assert("three" == Await.result(trans.read(), 5.seconds))

    // no reads from the channel yet
    verify(channel, never).read()


    // the offer q is drained, so the underlying channel is finally read
    trans.read()
    eventually { verify(channel, times(1)).read() }
  }
}
