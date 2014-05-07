package com.twitter.finagle.smtp

import javax.mail.internet.{InternetAddress, MimeMessage}
import javax.mail.Session
import javax.mail.Message
import java.util.Properties
import org.apache.commons.mail.SimpleEmail
import com.twitter.util.{Await, Future}
import com.twitter.finagle.Service

/**
 * Simple SMTP client without error handling
 */
object SimpleSMTPClient {
 def main(args: Array[String]) = {
   //raw text email
   val email1 = EmailMessage(
   "ex@ex.com",
   Seq("to@ro.com"),
   Seq(),
   Seq(),
   Seq("test")
   )

   //email from javamail
   val javamail = new MimeMessage(Session.getDefaultInstance(new Properties()))
   javamail.setFrom(new InternetAddress("ex@ex.com"))
   javamail.addRecipient(Message.RecipientType.TO, new InternetAddress("to@ro.com"))
   javamail.setSubject("test")
   javamail.setText("test")
   val email2 = EmailMessage(javamail)

   //email from a.c.e.
   val commons = new SimpleEmail
   commons.setFrom("ex@ex.com")
   commons.addTo("to@ro.com")
   commons.setSubject("test")
   commons.setMsg("test")

   val email3 = EmailMessage(commons)

   /*Mail(email3) onSuccess {
     case resps => for ((req, resp) <- resps) println(req + "\n" + resp)
   }*/

   val send = Smtp.newService("localhost:25")
   val res: Future[List[(String, String)]] = send(email3) onSuccess {
    case resps => for ((req, resp) <- resps) println(req + "\n" + resp)
   }

   println("Sending email...") //this will be printed before the future returns

   //blocking just for test purposes
   Await.ready(res)
 }
 }
