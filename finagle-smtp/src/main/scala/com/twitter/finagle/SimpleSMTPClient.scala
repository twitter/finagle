package com.twitter.finagle

/**
 * Simple SMTP client without error handling
 */
object SimpleSMTPClient {
 def main(args: Array[String]) = {
   val email = Email(
   "ex@ex.com",
   "to@ro.com",
   "from: ex@ex.com\n" +
     "to: to@ro.com\n" +
     "subject: test\n" +
     "\n" +
     "test\n"
   )


   val response = Mail(email) onSuccess {case resps => for ((req, resp) <- resps) println(req + "\n" + resp)}
   println("Sending email...") //this will be printed before the future returns
 }
 }
