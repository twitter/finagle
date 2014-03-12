package com.twitter.finagle

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.util.Future

/**
 * Simple mail service sending data to local SMTP server without error handling
 */
object Mail extends Service [Email, List[(String,String)]]{
   lazy val send = ClientBuilder()
   .hosts("localhost:25")
   .codec(new StringCodec)
   .hostConnectionLimit(1)
   .build()

     def apply(msg: Email) = msg match {
     case Email(from, to, body) => {
       val reqs = List(
         "EHLO",
         "MAIL FROM: <" + from + ">",
         "RCPT TO: <" + to + ">",
         "DATA",
         body + "\n.",
         "QUIT"
       )
        val freqs = for (req <- reqs) yield send(req)

        val fresps = Future.collect(freqs)

        fresps map {reqs zip _}
     }
   }
 }
