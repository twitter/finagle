package com.twitter.finagle.smtp

trait Request

private[smtp] class SingleRequest(val cmd: String) extends Request {
  override def toString = cmd
}

object Request {
  val Hello = new SingleRequest("EHLO") //Get information about the server
  val Quit = new SingleRequest("QUIT")  //Close connection
  val Reset = new SingleRequest("RSET") //Reset mailing session, returning to initial state
  val Noop = new SingleRequest("NOOP")  //Wait an OK response from server
  val BeginData = new SingleRequest("DATA") //Indicate that data is sent
}

case class AddFrom(addr: Seq[MailingAddress]) extends SingleRequest("MAIL FROM: <" + addr.mkString(",") + ">")
case class AddRecipients(rcpt: Seq[MailingAddress]) extends SingleRequest("RCPT TO: <" + rcpt.mkString(",") + ">")

case class Data(data: Seq[String]) extends SingleRequest(data.mkString("\r\n"))

case class VerifyAddress(address: MailingAddress) extends SingleRequest("VRFY " + address.toString)
case class ExpandMailingList(list: MailingAddress) extends SingleRequest("EXPN " + list.toString)

private[smtp] case class ComposedRequest(requests: Seq[SingleRequest]) extends Request

private[smtp] object ComposedRequest {
  def SendEmail(msg: EmailMessage) = ComposedRequest(Seq(
    AddFrom(msg.getFrom),
    AddRecipients(msg.getTo),
    Request.BeginData,
    Data(msg.getBody)
  ))
}


