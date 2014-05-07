package com.twitter.finagle.smtp

import com.twitter.finagle.{Name, Service, Filter, Client}
import com.twitter.util.Future
import org.jboss.netty.channel._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.client.{DefaultClient, Bridge}
import com.twitter.finagle.dispatch.SerialClientDispatcher

/*Filter for parsing email and sending corresponding commands, then aggregating results*/
object MailFilter extends Filter[EmailMessage, List[(String,String)], String, String]{
  override def apply(msg: EmailMessage, send: Service[String, String]): Future[List[(String, String)]] = {
    val reqs = List(
      "EHLO",
      "MAIL FROM: <" + msg.from + ">",
      "RCPT TO: <" + msg.to.mkString(",") + ">",
      "DATA",
      msg.body.mkString("\r\n")
        + "\r\n.",
      "QUIT"
    )
    val freqs = for (req <- reqs) yield send(req)

    val fresps = Future.collect(freqs)

    fresps map {reqs zip _}
  }
}

/*Temporary classes for string protocol*/
object StringClientPipeline extends ChannelPipelineFactory {
  def getPipeline = {
    val pipeline = Channels.pipeline()
    pipeline.addLast("stringEncode", new StringEncoder(CharsetUtil.UTF_8))
    pipeline.addLast("stringDecode", new StringDecoder(CharsetUtil.UTF_8))
    pipeline.addLast("line", new DelimEncoder('\n'))
    pipeline
  }
}

class DelimEncoder(delim: Char) extends SimpleChannelHandler {
  override def writeRequested(ctx: ChannelHandlerContext, evt: MessageEvent) = {
    val newMessage = evt.getMessage match {
      case m: String => m + delim
      case m => m
    }
    Channels.write(ctx, evt.getFuture, newMessage, evt.getRemoteAddress)
  }
}

object StringClientTransporter extends Netty3Transporter[String, String](
  name = "StringClientTransporter",
  pipelineFactory = StringClientPipeline)
/*---*/

object Smtp extends Client[EmailMessage, List[(String,String)]]{
  val defaultClient = DefaultClient[String, String](
    name = "smtp",
    endpointer = {
      val bridge = Bridge[String, String, String, String](
        StringClientTransporter, new SerialClientDispatcher(_)
      )
      (addr, stats) => bridge(addr, stats)
    }
  )

  override def newClient(dest: Name, label: String) = {
    MailFilter andThen defaultClient.newClient(dest, label)
  }

}
