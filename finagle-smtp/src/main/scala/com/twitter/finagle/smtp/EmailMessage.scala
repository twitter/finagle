package com.twitter.finagle.smtp

import java.util.Date

class MailingAddress(val local: String, val domain: String) {
  def mailbox: String = if (isEmpty) ""
                        else local + "@" + domain
  def isEmpty = local.isEmpty || domain.isEmpty
}

object MailingAddress {
  def apply(address: String) = {
    val parts = address split "@"
    require(parts.length == 2, "incorrect mailbox syntax")
    new MailingAddress(parts(0), parts(1))
  }
  val empty = new MailingAddress("","")
  def mailboxList(addrs: Seq[MailingAddress]) = addrs.filter(!_.isEmpty).map(_.mailbox).mkString(",")
}

trait EmailMessage {
  def getFrom: Seq[MailingAddress]
  def getSender: MailingAddress
  def getTo: Seq[MailingAddress]
  def getCc: Seq[MailingAddress]
  def getBcc: Seq[MailingAddress]
  def getReplyTo: Seq[MailingAddress]
  def getDate: Date
  def getSubject: String
  def getBody: Seq[String]
}
