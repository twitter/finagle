package com.twitter.finagle

import com.twitter.finagle.client.{DefaultClient, Bridge}
import com.twitter.finagle.smtp._
import com.twitter.finagle.smtp.filter.{MailFilter, HeadersFilter, DataFilter}
import com.twitter.finagle.smtp.reply._
import com.twitter.finagle.smtp.transport.SmtpTransporter
import com.twitter.util.{Time, Future}

// TODO: switch to StackClient

/**
 * Implements an SMTP client. This type of client is capable of sending
 * separate SMTP commands and receiving replies to them.
 */
object Smtp extends Client[Request, Reply]{

  private[this] val defaultClient = DefaultClient[Request, Reply] (
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
  override def newClient(dest: Name, label: String): ServiceFactory[Request, Reply] = {

    val quitOnCloseClient = {
      new ServiceFactoryProxy[Request, Reply](defaultClient.newClient(dest, label)) {
        override def apply(conn: ClientConnection): Future[ServiceProxy[Request, Reply]] = {
          self.apply(conn) map { service =>
            val quitOnClose = new ServiceProxy[Request, Reply](service) {
              override def close(deadline: Time): Future[Unit] = {
                if (service.isAvailable)
                  service(Request.Quit).unit
                else
                  Future.Done
              } ensure {
                service.close(deadline)
              }
            }
            quitOnClose
          }
        }
      }
    }

    DataFilter andThen quitOnCloseClient
  }
}

/**
 * Implements an SMTP client that can send an [[com.twitter.finagle.smtp.EmailMessage]].
 * The application of this client's service returns [[com.twitter.util.Future.Done]]
 * in case of success or the first encountered error in case of a failure.
 */
object SmtpSimple extends Client[EmailMessage, Unit] {
  /**
  * Constructs an SMTP client that sends a hello request
  * in the beginning of the session to identify itself;
  * it also copies email headers into the body of the message.
  * The dot stuffing and connection closing
  * behaviour is the same as in [[com.twitter.finagle.Smtp.newClient()]].
  */
  override def newClient(dest: Name, label: String): ServiceFactory[EmailMessage, Unit] = {
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


