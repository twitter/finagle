package com.twitter.finagle.transport

import org.specs.SpecificationWithJUnit
import org.mockito.{Matchers, ArgumentCaptor}
import org.specs.mock.Mockito
import org.jboss.netty.channel._
import com.twitter.util.{Return, Throw}
import com.twitter.finagle.ChannelException
import java.net.SocketAddress

class ChannelTransportSpec extends SpecificationWithJUnit with Mockito {
  "ChannelTransport" should {
    val ch = mock[Channel]
    val closeFuture = mock[ChannelFuture]
    ch.getCloseFuture returns closeFuture
    val remoteAddress = mock[SocketAddress]
    ch.getRemoteAddress returns remoteAddress
    val pipeline = new DefaultChannelPipeline
    val sink = mock[ChannelSink]
    ch.getPipeline returns pipeline
    pipeline.attach(ch, sink)
    val trans = new ChannelTransport[String, String](ch)

    def sendUpstream(e: ChannelEvent) {
      val handler = pipeline.getLast.asInstanceOf[ChannelUpstreamHandler]
      val ctx = mock[ChannelHandlerContext]
      handler.handleUpstream(ctx, e)
    }

    def sendUpstreamMessage(msg: Object) =
      sendUpstream({
        val e = mock[MessageEvent]
        e.getMessage returns msg
        e
      })

    "write to the underlying channel, proxying the underlying ChannelFuture" in {
      val f = trans.write("one")
      f.isDefined must beFalse
      val captor = ArgumentCaptor.forClass(classOf[ChannelEvent])
      there was one(sink).eventSunk(Matchers.eq(pipeline), captor.capture)
      captor.getValue must haveClass[DownstreamMessageEvent]
      val dsme = captor.getValue.asInstanceOf[DownstreamMessageEvent]
      dsme.getMessage must be_==("one")
      "success" in {
        dsme.getFuture.setSuccess()
        f.poll must beSome(Return(()))
      }
      "failure" in {
        val exc = new Exception("wtf")
        dsme.getFuture.setFailure(exc)
        f.poll must beSome(Throw(ChannelException(exc, remoteAddress)))
      }
    }

    "service reads" in {
      "before read()" in {
        sendUpstreamMessage("a reply!")
        trans.read().poll must beSome(Return("a reply!"))
      }

      "after read()" in {
        val f = trans.read()
        f.isDefined must beFalse
        sendUpstreamMessage("a reply!")
        f.poll must beSome(Return("a reply!"))
      }

      "queue up messages in FIFO order" in {
        for (i <- 0 until 10)
          sendUpstreamMessage("message:%d".format(i))

        for (i <- 0 until 10)
          trans.read().poll must beSome(Return("message:%d".format(i)))

        trans.read().isDefined must beFalse
      }
    }

    "handle exceptions" in {
      val exc = new Exception("sad panda")
      "on subsequent operations" in {
        sendUpstream({
          val e = mock[ExceptionEvent]
          e.getCause returns exc
          e
        })
        trans.read().poll must beSome(Throw(ChannelException(exc, remoteAddress)))
      }

      "on pending reads" in {  // writes are taken care of by netty
        val f = trans.read()
        f.isDefined must beFalse
        sendUpstream({
          val e = mock[ExceptionEvent]
          e.getCause returns exc
          e
        })
        f.poll must beSome(Throw(ChannelException(exc, remoteAddress)))
      }
    }

    "satisfy onClose" in {
      "when excepting" in {
        trans.onClose.poll must beNone
        val exc = new Exception("close exception")
        sendUpstream({
          val e = mock[ExceptionEvent]
          e.getCause returns exc
          e
        })
        trans.onClose.poll must beSome(Return(ChannelException(exc, remoteAddress)))
      }
    }
  }
}
