package com.twitter.finagle.smtp.filter

import com.twitter.finagle.{Service, Filter}
import com.twitter.finagle.smtp._
import com.twitter.util.Future
import com.twitter.finagle.smtp.reply.Reply
import com.twitter.finagle.smtp.Data

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
