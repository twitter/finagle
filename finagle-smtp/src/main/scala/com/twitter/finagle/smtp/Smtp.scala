package com.twitter.finagle.smtp

import com.twitter.finagle.{Name, Service, Filter, SimpleFilter, Client}
import com.twitter.util.Future
import com.twitter.finagle.client.{DefaultClient, Bridge}
import java.text.SimpleDateFormat
import java.util.Locale

object DataFilter extends SimpleFilter[Request, Reply] {
  override def apply(req: Request, send: Service[Request, Reply]): Future[Reply] = req match {
    case Data(lines: Seq[String]) => {
      //duplicate leading dot
      val firstLine = if (lines(0) == ".") ".." else lines(0)
      //add dot at the end
      val lastLine = "\r\n."
      val body = Seq(firstLine) ++ (lines drop 1) ++ Seq(lastLine)
      send(Data(body))
    }
    case other => send(other)
  }
}

object HeadersFilter extends SimpleFilter[EmailMessage, Unit] {
  def apply(msg: EmailMessage, send: Service[EmailMessage, Unit]): Future[Unit] = {
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
object MailFilter extends Filter[EmailMessage, Unit, Request, Reply]{
  override def apply(msg: EmailMessage, send: Service[Request, Reply]): Future[Unit] = {
    val SendEmailRequest: Seq[Request] =
      Seq(AddFrom(msg.getSender)) ++
      msg.getTo.map(AddRecipient(_)) ++
      Seq(Request.BeginData,
          Data(msg.getBody))

    val reqs: Seq[Request] =
      Seq(Request.Hello) ++
      SendEmailRequest ++
      Seq(Request.Quit)

    val freqs = for (req <- reqs) yield send(req)

    Future.collect(freqs) flatMap { _ =>
      Future.Done
    }

  }
}

object Smtp extends Client[Request, Reply]{

  val defaultClient = DefaultClient[Request, Reply] (
    name = "smtp",
    endpointer = {
      val bridge = Bridge[Request, UnspecifiedReply, Request, Reply](
        SmtpTransporter, new SmtpClientDispatcher(_)
      )
      (addr, stats) => bridge(addr, stats)
    })

  override def newClient(dest: Name, label: String) = {
    DataFilter andThen defaultClient.newClient(dest, label)
  }
}

object SmtpSimple extends Client[EmailMessage, Unit] {
  override def newClient(dest: Name, label: String) = {
    HeadersFilter andThen MailFilter andThen Smtp.newClient(dest, label)
  }
}
