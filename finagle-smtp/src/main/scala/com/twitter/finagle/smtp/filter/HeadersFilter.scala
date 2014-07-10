package com.twitter.finagle.smtp.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.Future
import java.text.SimpleDateFormat
import java.util.Locale
import com.twitter.finagle.smtp.{MailingAddress, EmailBuilder, EmailMessage}

object HeadersFilter extends SimpleFilter[EmailMessage, Unit] {
   def apply(msg: EmailMessage, send: Service[EmailMessage, Unit]): Future[Unit] = {
     val fields = Seq(
         "Date: " + new SimpleDateFormat("EE, dd MMM yyyy HH:mm:ss ZZ", Locale.forLanguageTag("eng")).format(msg.getDate),
         "From: " + MailingAddress.mailboxList(msg.getFrom),
         "Sender: " + msg.getSender.mailbox,
         "To: " + MailingAddress.mailboxList(msg.getTo),
         "Cc: " + MailingAddress.mailboxList(msg.getCc),
         "Bcc: " + MailingAddress.mailboxList(msg.getBcc),
         "Reply-To: " + MailingAddress.mailboxList(msg.getReplyTo),
         "Subject: " + msg.getSubject
       ).filter(_.split(": ").length > 1)

     val richmsg = EmailBuilder(msg)
                   .setBodyLines(fields ++ msg.getBody)
                   .build

     send(richmsg)
   }
 }
