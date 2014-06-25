package com.twitter.finagle.smtp

class Request(val cmd: String) {
  override def toString = cmd
}

object Request {
  val Hello = new Request("EHLO") //Get information about the server
  val Quit = new Request("QUIT")  //Close connection
  val Reset = new Request("RSET") //Reset mailing session, returning to initial state
  val Noop = new Request("NOOP")  //Wait an OK response from server
  val BeginData = new Request("DATA") //Indicate that data is sent


case class AddFrom(addr: MailingAddress) extends Request("MAIL FROM: <" + addr.toString + ">")
case class AddRecipient(rcpt: MailingAddress) extends Request("RCPT TO: <" + rcpt.toString + ">")

case class Data(data: Seq[String]) extends Request(data.mkString("\r\n"))

case class VerifyAddress(address: MailingAddress) extends Request("VRFY " + address.toString)
case class ExpandMailingList(list: MailingAddress) extends Request("EXPN " + list.toString)
}
