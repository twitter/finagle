package com.twitter.finagle.example.smtp

import com.twitter.util.{Await, Future}
import com.twitter.finagle.smtp._
import com.twitter.finagle.SmtpSimple
import com.twitter.logging.Logger

/**
 * Simple SMTP client with an example of error handling.
 */
object Example {
  private val log = Logger.get(getClass)

  def main(args: Array[String]) = {
    // Raw text email
    val email = EmailBuilder()
                .sender("from@from.com")
                .to("first@to.com", "second@to.com")
                .subject("test")
                .addBodyLines("first line", "second line") //body is a sequence of lines
                .build

    // Connect to a local SMTP server
    val send = SmtpSimple.newService("localhost:25")

    // Send email
    val res: Future[Unit] = send(email)
      .onFailure {
      // An error group
      case ex: reply.SyntaxErrorReply => log.error("Syntax error: ", ex.info)

      // A concrete reply
      case reply.ProcessingError(info) => log.error("Error processing request: ", info)
    }

    println("Sending email...") // This will be printed before the future returns

    Await.ready(res)

    println("Sent")
  }
}
