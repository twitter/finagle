package com.twitter.finagle.smtp

import java.text.SimpleDateFormat
import java.util.{Calendar, Locale, Date}

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
  def headers: Seq[(String, String)]

  def from: Seq[MailingAddress] = headers collect { case ("From", addr) => MailingAddress(addr) }

  def sender: MailingAddress = {
    if (from.length > 1) {
      headers collectFirst { case ("Sender", addr) => MailingAddress(addr) } getOrElse from.head
    }
    else if (from.length == 0) MailingAddress.empty
    else from.head
  }

  def to: Seq[MailingAddress] = headers collect { case ("To", addr) => MailingAddress(addr) }

  def cc: Seq[MailingAddress] = headers collect { case ("Cc", addr) => MailingAddress(addr) }

  def bcc: Seq[MailingAddress] = headers collect { case ("Bcc", addr) => MailingAddress(addr) }

  def replyTo: Seq[MailingAddress] = headers collect { case ("Reply-To", addr) => MailingAddress(addr) }

  def date: Date = {
    headers collectFirst {
      case ("Date", d) => EmailMessage.DateFormat.parse(d)
    } getOrElse Calendar.getInstance().getTime
  }

  def subject: String = headers collectFirst { case ("Subject", subj) => subj } getOrElse ""

  def body: Seq[String]
}

object EmailMessage {
  val DateFormat = new SimpleDateFormat("EE, dd MMM yyyy HH:mm:ss ZZ", Locale.forLanguageTag("eng"))
  def currentTime = DateFormat format Calendar.getInstance().getTime
}