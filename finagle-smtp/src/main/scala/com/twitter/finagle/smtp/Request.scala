package com.twitter.finagle.smtp

import java.net.InetAddress

/**
 * Represents SMTP request.
 *
 * @param cmd Text command to send to server
 */
class Request(val cmd: String) {
  override def toString(): String = cmd
}

/**
 * Contains subclasses and instances of known SMTP requests.
 */
object Request {
  /** Identifies the client */
  val Hello = new Request("EHLO " + InetAddress.getLocalHost.getHostName)

  /** Tells the server to close connection */
  val Quit = new Request("QUIT")

  /** Resets session */
  val Reset = new Request("RSET")

  /** Requests OK reply from the server */
  val Noop = new Request("NOOP")

  /** Indicates that email body is going to be sent */
  val BeginData = new Request("DATA")

  /** Starts mailing session and indicates sender mailbox */
  case class AddSender(addr: MailingAddress) extends Request("MAIL FROM: <" + addr.mailbox + ">")

  /** Adds a new recipient mailbox */
  case class AddRecipient(rcpt: MailingAddress) extends Request("RCPT TO: <" + rcpt.mailbox + ">")

  /** Sends the body of the message */
  case class Data(data: Seq[String]) extends Request(data.mkString("\r\n"))

  /** Requests the name of the user with given mailbox, if found */
  case class VerifyAddress(address: MailingAddress) extends Request("VRFY " + address.mailbox)

  /** Requests mailboxes included in a mailing list */
  case class ExpandMailingList(list: MailingAddress) extends Request("EXPN " + list.mailbox)
}
