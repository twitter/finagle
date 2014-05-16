package com.twitter.finagle.smtp

import com.twitter.finagle.{Name, Service, Filter, Client}
import com.twitter.util.Future
import org.jboss.netty.channel._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.client.{DefaultClient, Bridge}
import com.twitter.finagle.dispatch.SerialClientDispatcher

//initial response types
object resp {
  type SmtpResponse = String
  type SmtpResult = Seq[(Request, SmtpResponse)]
}

import resp._

//temporarily using filter for everything to work
object CommandFilter extends  Filter[Request, SmtpResult, String, String] {
  override def apply(req: Request, send: Service[String, String]): Future[SmtpResult] = req match {
    case SingleRequest(cmd) => {
      send(cmd) map {resp => Seq((req, resp))}
    }

    case ComposedRequest(reqs) => {
      val freqs = for (SingleRequest(cmd) <- reqs) yield send(cmd)
      val fresps = Future.collect(freqs)
      fresps map {resps => reqs zip resps}
    }
  }
}

/*Filter for parsing email and sending corresponding commands, then aggregating results*/
object MailFilter extends Filter[EmailMessage, SmtpResult, Request, SmtpResult]{
  override def apply(msg: EmailMessage, send: Service[Request, SmtpResult]): Future[SmtpResult] = {
    val reqs = Seq[Request](
      SingleRequest.Hello,
      ComposedRequest.SendEmail(msg),
      SingleRequest.Quit
    )
    val freqs = for (req <- reqs) yield send(req)

    val fresps = Future.collect(freqs)

    fresps.map(_.flatten) //compose result from several ones
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


object Smtp extends Client[Request, SmtpResult]{

  val defaultClient = DefaultClient[String, String] (
    name = "smtp",
    endpointer = {
      val bridge = Bridge[String, String, String, String](
        StringClientTransporter, new SerialClientDispatcher(_)
      )
      (addr, stats) => bridge(addr, stats)
    })

  override def newClient(dest: Name, label: String) = {
    CommandFilter andThen defaultClient.newClient(dest, label)
  }
}

object SmtpSimple extends Client[EmailMessage, SmtpResult] {
  override def newClient(dest: Name, label: String) = {
    MailFilter andThen Smtp.newClient(dest, label)
  }
}
