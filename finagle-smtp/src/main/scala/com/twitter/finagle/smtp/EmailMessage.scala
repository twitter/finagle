package com.twitter.finagle.smtp

import java.util.Date

private[smtp] class MailingAddress(val local: String, val domain: String) {
  def mailbox: String = if (isEmpty) ""
                        else local + "@" + domain
  def isEmpty = local.isEmpty && domain.isEmpty
}

object MailingAddress {
  def apply(address: String) = {
    val parts = address split "@"
    require(parts.length == 2, "incorrect mailbox syntax")
    require(!parts(0).isEmpty, "incorrect mailbox syntax: local part should not be empty")
    require(!parts(1).isEmpty, "incorrect mailbox syntax: domain should not be empty")
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
