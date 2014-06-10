package com.twitter.finagle.smtp

import com.twitter.finagle.{Name, Service, Filter, SimpleFilter, Client}
import com.twitter.util.Future
import com.twitter.finagle.client.{DefaultClient, Bridge}
import java.text.SimpleDateFormat
import java.util.Locale

//initial response types
object resp {
  type Result = Seq[(Request, Reply)]
}

import resp._

/*Filter for potentially sending sequences of requests*/
object CommandFilter extends  Filter[Request, Result, Request, Reply] {
  override def apply(req: Request, send: Service[Request, Reply]): Future[Result] = req match {
    case single: SingleRequest => {
      send(single) map {resp => Seq((single, resp))}
    }

    case ComposedRequest(reqs) => {
      val freqs = for (req <- reqs) yield send(req)
      val fresps = Future.collect(freqs)
      fresps map {resps => reqs zip resps}
    }
  }
}

object DataFilter extends SimpleFilter[Request, Reply] {
  override def apply(req: Request, send: Service[Request, Reply]): Future[Reply] = req match {
    case Data(lines: Seq[String]) => {
      //duplicate leading dot
      val firstLine = if (lines(0) == ".") ".." else lines(0)
      //add dot an the end
      val lastLine = "\r\n."
      val body = Seq(firstLine) ++ (lines drop 1) ++ Seq(lastLine)
      send(Data(body))
    }
    case other => send(other)
  }
}


object HeadersFilter extends SimpleFilter[EmailMessage, Result] {
  def apply(msg: EmailMessage, send: Service[EmailMessage, Result]): Future[Result] = {
    val date = "Date: " + new SimpleDateFormat("EE, dd MMM yyyy HH:mm:ss ZZ", Locale.forLanguageTag("eng")).format(msg.getDate)
    val from = "From: " + msg.getFrom.mkString(",")
    val sender = if (msg.getFrom.length > 1)"Sender: " + msg.getSender.toString else null
    val to = "To: " + msg.getTo.mkString(",")
    val subject = "Subject: " + msg.getSubject
    val fields = for (field <- Seq(date, from, sender, to, subject) if field != null) yield field
    val richmsg = EmailMessage(msg.getFrom, msg.getSender, msg.getTo, msg.getCc, msg.getBcc, msg.getDate, msg.getSubject, fields ++ msg.getBody)

    send(richmsg)
  }
}

/*Filter for parsing email and sending corresponding commands, then aggregating results*/
object MailFilter extends Filter[EmailMessage, Result, Request, Result]{
  override def apply(msg: EmailMessage, send: Service[Request, Result]): Future[Result] = {
    val reqs = Seq[Request](
      Request.Hello,
      ComposedRequest.SendEmail(msg),
      Request.Quit
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
      val bridge = Bridge[Request, UnspecifiedReply, Request, Reply](
        SmtpTransporter, new SmtpClientDispatcher(_)
      )
      (addr, stats) => bridge(addr, stats)
    })

  override def newClient(dest: Name, label: String) = {
    CommandFilter andThen DataFilter andThen defaultClient.newClient(dest, label)
  }
}

object SmtpSimple extends Client[EmailMessage, Result] {
  override def newClient(dest: Name, label: String) = {
    HeadersFilter andThen MailFilter andThen Smtp.newClient(dest, label)
  }
}
