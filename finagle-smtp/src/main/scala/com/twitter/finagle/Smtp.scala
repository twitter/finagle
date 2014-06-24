package com.twitter.finagle

import com.twitter.util.{Await, Future}
import com.twitter.finagle.client.{DefaultClient, Bridge}
import com.twitter.finagle.smtp._
import com.twitter.finagle.smtp.reply._
import com.twitter.finagle.smtp.filter.{MailFilter, HeadersFilter, DataFilter}
import com.twitter.finagle.smtp.transport.SmtpTransporter
import com.twitter.finagle.service.RetryingFilter


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


