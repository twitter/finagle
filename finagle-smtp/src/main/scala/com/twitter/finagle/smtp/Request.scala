package com.twitter.finagle.smtp

import java.net.InetAddress

class Request(val cmd: String) {
  override def toString = cmd
}

object Request {
  val Hello = new Request("EHLO " + InetAddress.getLocalHost.getHostName) //Get information about the server
  val Quit = new Request("QUIT")  //Close connection
  val Reset = new Request("RSET") //Reset mailing session, returning to initial state
  val Noop = new Request("NOOP")  //Wait an OK response from server
  val BeginData = new Request("DATA") //Indicate that data is sent


case class AddSender(addr: MailingAddress) extends Request("MAIL FROM: <" + addr.mailbox + ">")
case class AddRecipient(rcpt: MailingAddress) extends Request("RCPT TO: <" + rcpt.mailbox + ">")

case class Data(data: Seq[String]) extends Request(data.mkString("\r\n"))

case class VerifyAddress(address: MailingAddress) extends Request("VRFY " + address.mailbox)
case class ExpandMailingList(list: MailingAddress) extends Request("EXPN " + list.mailbox)
}
