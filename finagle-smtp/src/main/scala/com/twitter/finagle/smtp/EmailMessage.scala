package com.twitter.finagle.smtp

import java.util.Date

/** Defines email address */
private[smtp] class MailingAddress(val local: String, val domain: String) {
  def mailbox: String = if (isEmpty) ""
                        else local + "@" + domain
  def isEmpty = local.isEmpty && domain.isEmpty
}

/** Factory for mailing addresses. */
object MailingAddress {
  /**
   * Create a [[com.twitter.finagle.smtp.MailingAddress]] from the given string.
   * The given string representation of a mailbox
   * should be syntactically correct.
   *
   * @param address String representation of a mailbox
   */
  def apply(address: String) = {
    val parts = address split "@"
    require(parts.length == 2, "incorrect mailbox syntax")
    require(!parts(0).isEmpty, "incorrect mailbox syntax: local part should not be empty")
    require(!parts(1).isEmpty, "incorrect mailbox syntax: domain should not be empty")
    new MailingAddress(parts(0), parts(1))
  }

  /** An empty mailing address */
  val empty = new MailingAddress("","")

  /**
   * Creates a string representation of given list of mailboxes.
   *
   * @param addrs The addresses in the list
   * @return String containing string representation of given
   *         addresses divided with a comma.
   */
  def mailboxList(addrs: Seq[MailingAddress]) = addrs.filter(!_.isEmpty).map(_.mailbox).mkString(",")
}

/**
 * Defines fields and body of an email message.
 * */
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
