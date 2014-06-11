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
  import codecUtil._
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    val pipeline = ctx.getPipeline
    if (pipeline.get(aggregation) != null)
      pipeline.remove(aggregation)
    e.getMessage match {
      case rep: UnspecifiedReply => Channels.fireMessageReceived(ctx, rep)
      case other => Channels.fireMessageReceived(ctx, InvalidReply(other.toString))
    }
  }
}

private[smtp] object codecUtil {
  def getInfo(rep: String): String = rep drop 4
  def getCode(rep: String): Int = rep.take(3).toInt
  val aggregation = "aggregateMultiline"
}

class ReplyDecoder extends LineBasedFrameDecoder(100) {
import codecUtil._
  override def decode(ctx: ChannelHandlerContext, channel: Channel, msg: ChannelBuffer): UnspecifiedReply = {
    super.decode(ctx, channel, msg) match {
      case cb: ChannelBuffer => {
        val rep = cb.toString(CharsetUtil.UTF_8)
        val first = rep(0)
        val second = rep(1)
        val third = rep(2)

        //Standart reply: three-digit-code SP info
        if (first.isDigit && second.isDigit && third.isDigit)
          rep(3) match {
            case ' ' => 
              new UnspecifiedReply {
              val code = getCode(rep)
              val info = getInfo(rep)
              }

            case '-' =>
              val pipeline = ctx.getPipeline
              val code = getCode(rep)
              if (pipeline.get(aggregation) == null)
                pipeline.addBefore("smtpDecode", aggregation, AggregateMultiline(code, Seq[String]()))
              MultilinePart(code, getInfo(rep))


            case _ => InvalidReply(rep)
          }

        else
          InvalidReply(rep)
      }
    }
  }
}

/*Aggregates Extension replies in one AvailableExtensions reply*/
case class AggregateMultiline(multiline_code: Int, lns: Seq[String]) extends SimpleChannelUpstreamHandler {
  import codecUtil._
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) = {
    e.getMessage match {
      case MultilinePart(code, next) => 
        if (code == multiline_code){
        val pipeline = ctx.getPipeline
        pipeline.replace(aggregation, aggregation, copy(lns = lns :+ next))
      }
        else Channels.fireMessageReceived(ctx, InvalidReply(code.toString + "-" + next))
      //last element in the list
      case last: UnspecifiedReply => {
        val multiline = new UnspecifiedReply {
          val info: String = lns.head
          val code: Int = multiline_code
          override val lines = lns :+ last.info
          override val isMultiline = true
        }
        Channels.fireMessageReceived(ctx, multiline)
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

