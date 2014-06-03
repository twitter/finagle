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
  type Result = Seq[(Request, Reply)]
}

import resp._

/*Filter for potentially sending sequences of requests*/
object CommandFilter extends  Filter[Request, Result, Request, Reply] {
  override def apply(req: Request, send: Service[Request, Reply]): Future[Result] = req match {
    case SingleRequest(cmd) => {
      send(req) map {resp => Seq((req, resp))}
    }

    case ComposedRequest(reqs) => {
      val freqs = for (req <- reqs) yield send(req)
      val fresps = Future.collect(freqs)
      fresps map {resps => reqs zip resps}
    }
  }
}

/*Filter for parsing email and sending corresponding commands, then aggregating results*/
object MailFilter extends Filter[EmailMessage, Result, Request, Result]{
  override def apply(msg: EmailMessage, send: Service[Request, Result]): Future[Result] = {
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

object Smtp extends Client[Request, Result]{

  val defaultClient = DefaultClient[Request, Reply] (
    name = "smtp",
    endpointer = {
      val bridge = Bridge[String, Reply, Request, Reply](
        SmtpTransporter, new SmtpClientDispatcher(_)
      )
      (addr, stats) => bridge(addr, stats)
    })

  override def newClient(dest: Name, label: String) = {
    CommandFilter andThen defaultClient.newClient(dest, label)
  }
}

object SmtpSimple extends Client[EmailMessage, Result] {
  override def newClient(dest: Name, label: String) = {
    MailFilter andThen Smtp.newClient(dest, label)
  }
}
