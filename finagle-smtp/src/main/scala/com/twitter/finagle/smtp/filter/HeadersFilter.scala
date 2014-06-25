package com.twitter.finagle.smtp.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.Future
import java.text.SimpleDateFormat
import java.util.Locale
import com.twitter.finagle.smtp.EmailMessage

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
