package com.twitter.finagle

import com.twitter.finagle.smtp.EmailMessage
import com.twitter.util.{Await, Future}

/**
 * Simple SMTP client with an example of error handling
 */
object Example {
  def main(args: Array[String]) = {
    //raw text email
    val email1 = EmailMessage(
      from = "from@from.com",
      to = Seq("first@to.com", "second@to.com"),
      subject = "test",
      body = Seq("test") //body is a sequence of lines
    )
    //connect to a local SMTP server
    val send = SmtpSimple.newService("localhost:25")
    //send email
    val res: Future[Unit] = send(email1)
    //do some error handling
      .onFailure {
      //catching by reply group
      case ex: smtp.reply.SyntaxErrorReply => println("Syntax error: ", ex.info)
      //catching by concrete reply
      case smtp.reply.ProcessingError(info) => println("Error processing request: ", info)
    }

    println("Sending email...") //this will be printed before the future returns

    //blocking just for test purposes
    Await.ready(res)
    println("Sent")
  }
}
