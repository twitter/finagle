package com.twitter.finagle.smtp

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.util.Future
import com.twitter.finagle.Service

/**
 * Simple mail service sending data to local SMTP server without error handling
 */

object Mail extends Service [EmailMessage, List[(String,String)]]{
   lazy val send = ClientBuilder()
   .hosts("localhost:25")
   .codec(new StringCodec)
   .hostConnectionLimit(1)
   .build()

     def apply(msg: EmailMessage) = {
       val reqs = List(
         "EHLO",
         "MAIL FROM: <" + msg.from + ">",
         "RCPT TO: <" + msg.to.mkString(",") + ">",
         "DATA",
         msg.body.mkString("\r\n")
           + "\r\n.",
         "QUIT"
       )
        val freqs = for (req <- reqs) yield send(req)

        val fresps = Future.collect(freqs)

        fresps map {reqs zip _}
      }
 }
