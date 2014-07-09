package com.twitter.finagle.example.smtp

import com.twitter.util.{Await, Future}
import com.twitter.finagle.smtp._
import com.twitter.finagle.SmtpSimple

/**
 * Simple SMTP client with an example of error handling
 */
object Example {
  def main(args: Array[String]) = {
    //raw text email
    val email = EmailBuilder()
                .sender("from@from.com")
                .to("first@to.com", "second@to.com")
                .subject("test")
                .bodyLines("first line", "second line") //body is a sequence of lines
                .build
    //connect to a local SMTP server
    val send = SmtpSimple.newService("localhost:25")
    //send email
    val res: Future[Unit] = send(email)
      .onFailure {
      //An error group
      case ex: reply.SyntaxErrorReply => println("Syntax error: ", ex.info)
      //A concrete reply
      case reply.ProcessingError(info) => println("Error processing request: ", info)
    }

    println("Sending email...") //this will be printed before the future returns

    //blocking just for test purposes
    Await.ready(res)
    println("Sent")
  }
}
