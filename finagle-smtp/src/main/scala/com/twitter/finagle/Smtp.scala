package com.twitter.finagle

import com.twitter.util.{Time, Future}
import com.twitter.finagle.client.{DefaultClient, Bridge}
import com.twitter.finagle.smtp._
import com.twitter.finagle.smtp.reply._
import com.twitter.finagle.smtp.filter.{MailFilter, HeadersFilter, DataFilter}
import com.twitter.finagle.smtp.transport.SmtpTransporter

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

    val quitOnCloseClient = new ServiceFactoryProxy[Request, Reply](defaultClient.newClient(dest, label)){

      override def apply(conn: ClientConnection) = {
        self.apply(conn) flatMap { service =>
          val quitOnClose = new ServiceProxy[Request, Reply](service) {
            override def close(deadline: Time) = {
              if (service.isAvailable)
                  service(Request.Quit)
              service.close(deadline)
            }
          }
          Future.value(quitOnClose)
        }
      }
    }

    DataFilter andThen quitOnCloseClient
  }
}


object SmtpSimple extends Client[EmailMessage, Unit] {
  override def newClient(dest: Name, label: String) = {
    //send EHLO in the beginning of the session
    val startHelloClient = new ServiceFactoryProxy[Request, Reply](Smtp.newClient(dest, label)) {
      override def apply(conn: ClientConnection) = {
        self.apply(conn) flatMap { service =>
          service(Request.Hello) //TODO: parse and make use of extensions
          Future.value(service)
        }
      }
    }
    HeadersFilter andThen MailFilter andThen startHelloClient
  }
}


