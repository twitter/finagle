package com.twitter.finagle.smtp.filter

import com.twitter.finagle.{Service, Filter}
import com.twitter.finagle.smtp._
import com.twitter.util.Future
import com.twitter.finagle.smtp.reply.Reply

/*Filter for parsing email and sending corresponding commands, then aggregating results*/
object MailFilter extends Filter[EmailMessage, Unit, Request, Reply]{
   override def apply(msg: EmailMessage, send: Service[Request, Reply]): Future[Unit] = {
     val SendEmailRequest: Seq[Request] =
       Seq(Request.AddFrom(msg.getSender)) ++
       msg.getTo.map(Request.AddRecipient(_)) ++
       Seq(Request.BeginData,
           Request.Data(msg.getBody))

     val reqs: Seq[Request] =
       Seq(Request.Reset, Request.Hello) ++
       SendEmailRequest ++
       Seq(Request.Quit)

     val freqs = for (req <- reqs) yield send(req)

     Future.collect(freqs) flatMap { _ =>
       Future.Done
     }

   }
 }
