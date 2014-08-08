package com.twitter.finagle.example.smtp

import com.twitter.logging.Logger
import com.twitter.finagle.smtp._
import com.twitter.finagle.SmtpSimple
import com.twitter.util.{Await, Future}

/**
 * Simple SMTP client with an example of error handling.
 */
object Example {
  private val log = Logger.get(getClass)

  def main(args: Array[String]): Unit = {
    // Raw text email
    val email = EmailBuilder()
      .sender("from@from.com")
      .to("first@to.com", "second@to.com")
      .subject("test")
      .addBodyLines("first line", "second line") //body is a sequence of lines
      .build()

    // Connect to a local SMTP server
    val send = SmtpSimple.newService("localhost:2525")

    // Send email
    val res: Future[Unit] = send(email) onFailure {
    // An error group
    case ex: reply.SyntaxErrorReply => log.error("Syntax error: %s", ex.info)

    // A concrete reply
    case reply.ProcessingError(info) => log.error("Error processing request: %s", info)
    }

    log.info("Sending email...")

    Await.ready(res)

    log.info("Sent")
  }
}
