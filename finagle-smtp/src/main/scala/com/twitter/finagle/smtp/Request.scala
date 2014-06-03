package com.twitter.finagle.smtp

trait Request

case class SingleRequest(text: String) extends Request

object SingleRequest {
  val Hello = SingleRequest("EHLO") //Get information about the server
  val Quit = SingleRequest("QUIT")  //Close connection
  val Reset = SingleRequest("RSET") //Reset mailing session, returning to initial state
  val Noop = SingleRequest("NOOP")  //Wait an OK response from server

  def AddFrom(addr: String) = SingleRequest("MAIL FROM: <" + addr + ">")
  def AddRecipients(rcpt: Seq[String]) = SingleRequest("RCPT TO: <" + rcpt.mkString(",") + ">")

  val BeginData = SingleRequest("DATA") //Indicate that data is sent
  def Data(data: Seq[String]) = SingleRequest(data.mkString("\r\n"))
  val EndData = SingleRequest("\r\n.")

  def VerifyAddress(address: String) = SingleRequest("VRFY " + address)
  def ExpandMailingList(list: String) = SingleRequest("EXPN " + list)
}

private[smtp] case class ComposedRequest(requests: Seq[SingleRequest]) extends Request

private[smtp] object ComposedRequest {
  def SendEmail(msg: EmailMessage) = ComposedRequest(Seq(
    SingleRequest.AddFrom(msg.from),
    SingleRequest.AddRecipients(msg.to),
    SingleRequest.BeginData,
    SingleRequest.Data(msg.body),
    SingleRequest.EndData
  ))
}


