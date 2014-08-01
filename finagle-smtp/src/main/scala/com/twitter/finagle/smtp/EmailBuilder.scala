package com.twitter.finagle.smtp

import java.util.{Calendar, Date}

/** The payload of an email. */
case class Payload(from: Seq[String],
                   sender: String,
                   to: Seq[String],
                   cc: Seq[String],
                   bcc: Seq[String],
                   reply_to: Seq[String],
                   date: String,
                   subject: String,
                   body: Seq[String])


/**
 * Composes an [[com.twitter.finagle.smtp.EmailMessage]]. */
case class EmailBuilder(payload: Payload) {  
  /**
   * Adds originator addresses, which will appear in the 'From:' field.
   * 
   * @param addrs Addresses to be added
   */
  def from(addrs: String*): EmailBuilder = setFrom(payload.from ++ addrs)

  /**
   * Set the ''From:'' field to contain given originator addresses.
   *
   * @param addrs Addresses to be set
   */
  def setFrom(addrs: Seq[String]): EmailBuilder = copy(payload.copy(from = addrs))

  /**
   * Sets the ''Sender:'' field to contain given mailing address.
   * Sender should be specified if there are multiple 
   * originator addresses. 
   * If the given address is not included in ''From:'' field,
   * it is added there.
   *
   * @param addr Address to be set
   */
  def sender(addr: String): EmailBuilder = {
    if(payload.from contains addr) copy(payload.copy(sender = addr))
    else copy(payload.copy(sender = addr, from = payload.from :+ addr))
  }

  /**
   * Adds recipient addresses, which will appear in the ''To:'' field.
   *
   * @param addrs Addresses to be added
   */
  def to(addrs: String*): EmailBuilder = setTo(payload.to ++ addrs)

  /**
   * Set the ''To:'' field to contain given recipient addresses.
   *
   * @param addrs Addresses to be set
   */
  def setTo(addrs: Seq[String]): EmailBuilder = copy(payload.copy(to = addrs))

  /**
   * Adds carbon copy addresses, which will appear in the ''Cc:'' field.
   *
   * @param addrs Addresses to be added
   */
  def cc(addrs: String*): EmailBuilder = setCc(payload.cc ++ addrs)

  /**
   * Set the ''Cc:'' field to contain given carbon copy addresses.
   *
   * @param addrs Addresses to be set
   */
  def setCc(addrs: Seq[String]): EmailBuilder = copy(payload.copy(cc = addrs))

  /**
   * Adds blind carbon copy addresses, which will appear in the ''Bcc:'' field.
   *
   * @param addrs Addresses to be added
   */
  def bcc(addrs: String*): EmailBuilder = setBcc(payload.bcc ++ addrs)

  /**
   * Set the ''Bcc:'' field to contain given blind carbon copy addresses.
   *
   * @param addrs Addresses to be set
   */
  def setBcc(addrs: Seq[String]): EmailBuilder = copy(payload.copy(bcc = addrs))

  /**
   * Adds the addresses to reply to, which will appear in the ''Reply-To:'' field.
   *
   * @param addrs Addresses to be added
   */
  def reply_to(addrs: String*): EmailBuilder = setReplyTo(payload.reply_to ++ addrs)

  /**
   * Set the ''Reply-To:'' field to contain given addresses to reply to.
   *
   * @param addrs Addresses to be set
   */
  def setReplyTo(addrs: Seq[String]): EmailBuilder = copy(payload.copy(reply_to = addrs))

  /**
   * Set the date of sending the message.
   *
   * @param dt Date to be set
   */
  def date(dt: Date): EmailBuilder = copy(payload.copy(date = EmailMessage.DateFormat.format(dt)))

  /**
   * Set the subject of the message.
   *
   * @param sbj Subject to be set
   */
  def subject(sbj: String): EmailBuilder = copy(payload.copy(subject = sbj))

  /**
   * Add lines to the message text.
   *
   * @param lines Lines to be added
   */
  def addBodyLines(lines: String*): EmailBuilder = setBodyLines(payload.body ++ lines)

  /**
   * Add lines to the message text.
   *
   * @param lines Lines to be added
   */
  def setBodyLines(lines: Seq[String]): EmailBuilder = copy(payload.copy(body = lines))

  /**
   * Instantiate an [[com.twitter.finagle.smtp.EmailMessage]] from the payload.
   * If the date of sending the message is not set,
   * current date is used. If sender of the message
   * is not specified, the first address in ''From:'' is used.
   */
  def build: EmailMessage = new EmailMessage {
    def headers = {
      payload.from.map(("From",_)) ++
      Seq("Sender"   -> { if (payload.sender.nonEmpty) payload.sender
                          else payload.from.head }) ++
      payload.to.map(("To",_)) ++
      payload.cc.map(("Cc",_)) ++
      payload.bcc.map(("Bcc",_)) ++
      payload.reply_to.map(("Reply-To",_)) ++
      Seq(
        "Date"     -> { if (payload.date != null) payload.date
                        else EmailMessage.currentTime },
        "Subject"  -> payload.subject
      )
    }

    def body = payload.body
  }

}

/**
 * Factory for [[com.twitter.finagle.smtp.EmailBuilder]] instances
 */
object EmailBuilder {
  /**
   * Creates a default EmailBuilder with empty payload.
   */
  def apply() = new EmailBuilder(Payload(from = Seq.empty,
                                       sender = "",
                                       to = Seq.empty,
                                       cc = Seq.empty,
                                       bcc = Seq.empty,
                                       reply_to = Seq.empty,
                                       date = null,
                                       subject = "",
                                       body = Seq.empty))

  /**
   * Creates an [[com.twitter.finagle.smtp.EmailBuilder]] with payload from given [[com.twitter.finagle.smtp.EmailMessage]].
   *
   * @param msg The message to copy payload from
   */
  def apply(msg: EmailMessage) = new EmailBuilder(Payload(from = msg.from.map(_.mailbox),
                                                          sender = msg.sender.mailbox,
                                                          to = msg.to.map(_.mailbox),
                                                          cc = msg.cc.map(_.mailbox),
                                                          bcc = msg.bcc.map(_.mailbox),
                                                          reply_to = msg.replyTo.map(_.mailbox),
                                                          date = EmailMessage.DateFormat.format(msg.date),
                                                          subject = msg.subject,
                                                          body = msg.body))
}
