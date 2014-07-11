package com.twitter.finagle.smtp

import java.util.{Calendar, Date}

case class Payload(from: Seq[MailingAddress],
                   sender: MailingAddress,
                   to: Seq[MailingAddress],
                   cc: Seq[MailingAddress],
                   bcc: Seq[MailingAddress],
                   reply_to: Seq[MailingAddress],
                   date: Date,
                   subject: String,
                   body: Seq[String])

case class EmailBuilder(payload: Payload) {
  /*Add addrs to the From: field*/
  def from(addrs: String*): EmailBuilder = setFrom(payload.from ++ addrs.map(MailingAddress(_)))
  /*Set From: field to addrs*/
  def setFrom(addrs: Seq[MailingAddress]): EmailBuilder = copy(payload.copy(from = addrs))

  /*Set Sender: field to addr*/
  def sender(addr: MailingAddress): EmailBuilder = {
    if(payload.from contains addr) copy(payload.copy(sender = addr))
    else copy(payload.copy(sender = addr, from = payload.from :+ addr))
  }
  def sender(addr: String): EmailBuilder = sender(MailingAddress(addr))

  /*Add addrs to the To: field*/
  def to(addrs: String*): EmailBuilder = setTo(payload.to ++ addrs.map(MailingAddress(_)))
  /*Set To: field to addrs*/
  def setTo(addrs: Seq[MailingAddress]): EmailBuilder = copy(payload.copy(to = addrs))

  /*Add addrs to the Cc: field*/
  def cc(addrs: String*): EmailBuilder = setCc(payload.cc ++ addrs.map(MailingAddress(_)))
  /*Set Cc: field to addrs*/
  def setCc(addrs: Seq[MailingAddress]): EmailBuilder = copy(payload.copy(cc = addrs))

  /*Add addrs to the Bcc: field*/
  def bcc(addrs: String*): EmailBuilder = setBcc(payload.bcc ++ addrs.map(MailingAddress(_)))
  /*Set Bcc: field to addrs*/
  def setBcc(addrs: Seq[MailingAddress]): EmailBuilder = copy(payload.copy(bcc = addrs))

  /*Add addrs to the Reply-To: field*/
  def reply_to(addrs: String*): EmailBuilder = setReplyTo(payload.reply_to ++ addrs.map(MailingAddress(_)))
  /*Set Reply-To: field to addrs*/
  def setReplyTo(addrs: Seq[MailingAddress]): EmailBuilder = copy(payload.copy(reply_to = addrs))

  /*Set Date: field to dt*/
  def date(dt: Date): EmailBuilder = copy(payload.copy(date = dt))

  /*Set Subject: field to sbj*/
  def subject(sbj: String): EmailBuilder = copy(payload.copy(subject = sbj))

  /*Add lines to the body*/
  def bodyLines(lines: String*): EmailBuilder = setBodyLines(payload.body ++ lines)
  /*Set body to lines*/
  def setBodyLines(lines: Seq[String]): EmailBuilder = copy(payload.copy(body = lines))

  def build: EmailMessage = new EmailMessage {
    def getBcc = payload.bcc
    def getDate = if (payload.date == null) Calendar.getInstance().getTime
                  else payload.date
    def getCc = payload.cc
    def getTo = payload.to
    def getSubject = payload.subject
    def getSender = payload.sender
    def getBody = payload.body
    def getFrom = payload.from
    def getReplyTo = payload.reply_to
  }

}

object EmailBuilder {
  def apply() = new EmailBuilder(Payload(from = Seq.empty,
                                       sender = MailingAddress.empty,
                                       to = Seq.empty,
                                       cc = Seq.empty,
                                       bcc = Seq.empty,
                                       reply_to = Seq.empty,
                                       date = null,
                                       subject = "",
                                       body = Seq.empty))

  def apply(msg: EmailMessage) = new EmailBuilder(Payload(from = msg.getFrom,
                                                          sender = msg.getSender,
                                                          to = msg.getTo,
                                                          cc = msg.getCc,
                                                          bcc = msg.getBcc,
                                                          reply_to = msg.getReplyTo,
                                                          date = msg.getDate,
                                                          subject = msg.getSubject,
                                                          body = msg.getBody))
}
