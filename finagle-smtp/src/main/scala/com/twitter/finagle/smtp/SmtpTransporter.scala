package com.twitter.finagle.smtp

import org.jboss.netty.channel._
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.util.NonFatal
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.frame.LineBasedFrameDecoder
import org.jboss.netty.handler.logging.LoggingHandler
import org.jboss.netty.logging.InternalLogLevel

class SmtpDecoder extends SimpleChannelUpstreamHandler{
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val pipeline = ctx.getPipeline
    if (pipeline.get("makeGreeting") != null)
      pipeline.remove("makeGreeting")
    if (pipeline.get("aggregateExtensions") != null)
      pipeline.remove("aggregateExtensions")
    e.getMessage match {
      case rep: UnspecifiedReply => Channels.fireMessageReceived(ctx, rep)
      case other => Channels.fireMessageReceived(ctx, InvalidReply(other.toString))
    }
  }
}

object ReplyDecoder {
  private def getInfo(rep: String) = rep drop 4
}

class ReplyDecoder extends LineBasedFrameDecoder(100) {
import ReplyDecoder._
  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: ChannelBuffer): UnspecifiedReply = {
    super.decode(ctx, channel, msg) match {
      case cb: ChannelBuffer => {
        val rep = cb.toString(CharsetUtil.UTF_8)
        val first = rep(0)
        val second = rep(1)
        val third = rep(2)

        //connected, server greets the client
        if (rep.startsWith("220 ")) {
          val greet = getInfo(rep)
          //wait for another reply to pair with the greeting
          ctx.getPipeline.addBefore("smtpDecode", "makeGreeting", makeGreeting(greet))
          EmptyReply
        }
        //EHLO reply: list of extensions
        else if (rep.startsWith("250-")) {
          val pipeline = ctx.getPipeline
          val info = getInfo(rep)
          if (pipeline.get("aggregateExtensions") == null) {
            //wait for all list to be receive and wrap it into a reply
            if (pipeline.get("makeGreeting") != null) //possibly to pair with the greeting
              pipeline.addBefore("makeGreeting", "aggregateExtensions", AggregateExtensions(info, Seq[Extension]()))
            else
              pipeline.addBefore("smtpDecode","aggregateExtensions", AggregateExtensions(info, Seq[Extension]()))
            EmptyReply
          }
          else {
            Channels.fireMessageReceived(ctx, Extension(info))
            EmptyReply
          }
        }
        //specifying at this stage to be able to detect the last node in extension list
        else if (rep.startsWith("250 ")) OK(getInfo(rep))
        //Standart reply: three-digit-code SP info
        else if (first.isDigit && second.isDigit && third.isDigit && rep(3).isSpaceChar)
          new UnspecifiedReply {
            val code = rep.take(3).toInt
            val info = getInfo(rep)
          }
        else
          InvalidReply(rep)
      }
    }
  }
}

/*Waits for the next reply to pair it with greeting*/
case class makeGreeting(greet: String) extends SimpleChannelUpstreamHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case EmptyReply => {}
      case rep: Reply => {
        Channels.fireMessageReceived(ctx, Greeting(greet, rep))
      }
      case _ => ctx.sendUpstream(e)
    }
  }
}

/*Aggregates Extension replies in one AvailableExtensions reply*/
case class AggregateExtensions(info: String, ext: Seq[Extension]) extends SimpleChannelUpstreamHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case EmptyReply => {}
      case extension: Extension => {
        val pipeline = ctx.getPipeline
        pipeline.replace("aggregateExtensions", "aggregateExtensions", copy(ext = ext :+ extension))
      }
      //last element in the list
      case last: OK => {
        Channels.fireMessageReceived(ctx, AvailableExtensions(info, ext,  last))
      }
      case _ => ctx.sendUpstream(e)
    }
  }
}

class SmtpEncoder extends SimpleChannelDownstreamHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) =
    evt.getMessage match {
      case single: SingleRequest =>
        try {
          val buf = ChannelBuffers.copiedBuffer(single.cmd + "\r\n", CharsetUtil.UTF_8)
          Channels.write(ctx, evt.getFuture, buf, evt.getRemoteAddress)
        } catch {
          case NonFatal(e) =>
            evt.getFuture.setFailure(new ChannelException(e.getMessage))
        }

      case unknown =>
        evt.getFuture.setFailure(new ChannelException(
          "Unsupported request type %s".format(unknown.getClass.getName)))
    }
}

object SmtpPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    //pipeline.addLast("logger", new LoggingHandler(InternalLogLevel.INFO))
    pipeline.addLast("smtpEncode", new SmtpEncoder)
    pipeline.addLast("replyDecode", new ReplyDecoder)
    pipeline.addLast("smtpDecode", new SmtpDecoder)
    pipeline
  }
}

object SmtpTransporter extends Netty3Transporter[Request, UnspecifiedReply](
  name = "SmtpTransporter",
  pipelineFactory = SmtpPipeline)

