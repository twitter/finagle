package com.twitter.finagle

import com.twitter.util.{Time, Future}
import com.twitter.finagle.client.{DefaultClient, Bridge}
import com.twitter.finagle.smtp._
import com.twitter.finagle.smtp.reply._
import com.twitter.finagle.smtp.filter.{MailFilter, HeadersFilter, DataFilter}
import com.twitter.finagle.smtp.transport.SmtpTransporter


/**
 * Implements an SMTP client in terms of a finagle DefaultClient.
 * This type of client is capable of sending independent
 * SMTP commands and receiving replies to them.
 */
object Smtp extends Client[Request, Reply]{

  val defaultClient = DefaultClient[Request, Reply] (
    name = "smtp",
    endpointer = {
      val bridge = Bridge[Request, UnspecifiedReply, Request, Reply](
        SmtpTransporter, new SmtpClientDispatcher(_)
      )
      (addr, stats) => bridge(addr, stats)
    })

  /**
   * Constructs an SMTP client.
   *
   * Upon closing the connection this client sends QUIT command;
   * it also performs dot stuffing.
   */
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

/**
 * Implements an SMTP client that can send an [[com.twitter.finagle.smtp.EmailMessage]].
 * The application of this client's service doesn't return a
 * reply; instead, it returns [[Future.Done]] in case of success
 * or the first encountered error in case of a failure.
 */
object SmtpSimple extends Client[EmailMessage, Unit] {
  /**
  * Constructs an SMTP client that sends a hello request
  * in the beginning of the session to identify itself;
  * it also copies email headers into the body of the message.
  * The dot stuffing and connection closing
  * behaviour is the same as in [[Smtp.newClient()]].
  */
  override def newClient(dest: Name, label: String) = {
    val startHelloClient = new ServiceFactoryProxy[Request, Reply](Smtp.newClient(dest, label)) {
      override def apply(conn: ClientConnection) = {
        self.apply(conn) flatMap { service =>
          service(Request.Hello)
          Future.value(service)
        }
      }
    }
    HeadersFilter andThen MailFilter andThen startHelloClient
  }
}


