package com.twitter.finagle.smtp

//import javax.mail.internet.InternetAddress
//import javax.mail.{Session, Multipart, Message}
//import scala.collection.JavaConversions._
import java.util.{Calendar, Date, Properties}

class MailingAddress(val local: String, val domain: String) {
  override def toString = local + "@" + domain
}

object MailingAddress {
  def apply(address: String) = {
    val parts = address split "@"
    new MailingAddress(parts(0), parts(1))
  }
}

trait EmailMessage {
  def getFrom: Seq[MailingAddress]
  def getSender: MailingAddress
  def getTo: Seq[MailingAddress]
  def getCc: Seq[MailingAddress]
  def getBcc: Seq[MailingAddress]
  def getDate: Date
  def getSubject: String
  def getBody: Seq[String]
}

object EmailMessage {
  def apply(from: Seq[MailingAddress],
            sender: MailingAddress,
            to: Seq[MailingAddress],
            cc: Seq[MailingAddress],
            bcc: Seq[MailingAddress],
            date: Date,
            subject: String,
            body: Seq[String])
  = new EmailMessage{
    def getFrom = from
    def getSender = sender
    def getTo = to
    def getCc = cc
    def getBcc = bcc
    def getDate = date
    def getSubject = subject
    def getBody = body
  }

  def apply(from: Seq[MailingAddress],
            to: Seq[MailingAddress],
            cc: Seq[MailingAddress],
            bcc: Seq[MailingAddress],
            date: Date,
            subject: String,
            body: Seq[String]): EmailMessage
  = apply(from, from(0), to, cc, bcc, date, subject, body)

  def apply(from: Seq[MailingAddress],
            to: Seq[MailingAddress],
            subject: String,
            body: Seq[String]): EmailMessage
  = apply(from, from(0), to, Seq(), Seq(), Calendar.getInstance().getTime, subject, body)

  def apply(from: String,
            to: Seq[String],
            subject: String,
            body: Seq[String]): EmailMessage
  = apply(Seq(MailingAddress(from)), to map (MailingAddress(_)), subject, body)

  def apply(from: String,
            to: String,
            subject: String,
            body: Seq[String]): EmailMessage
  = apply(from, Seq(to), subject, body)
  //def apply(javamail: javax.mail.internet.MimeMessage) = new JavaMailMessage(javamail)

  //def apply(commonsmail: org.apache.commons.mail.Email) = new CommonsMailMessage(commonsmail)
}
/*
class JavaMailMessage(val javamail: javax.mail.internet.MimeMessage) extends EmailMessage {
  def from = InternetAddress.toString(javamail.getFrom)
  def to = javamail.getRecipients(Message.RecipientType.TO).map(_.toString)
  def cc = javamail.getRecipients(Message.RecipientType.CC).map(_.toString)
  def bcc = javamail.getRecipients(Message.RecipientType.BCC).map(_.toString)
  //suppose we have only text messages
  def body = javamail.getContent match {
    case content: String => content.split("\r\n")
    case content: Multipart => (for (i <- 1 to content.getCount) yield content.getBodyPart(i).getContent match{
      case part: String => part.split("\r\n")
      case _ => Array[String]()
    }).flatten
    case _ => Seq[String]()
  }
}

class CommonsMailMessage(val commonsmail: org.apache.commons.mail.Email) extends EmailMessage {
  commonsmail.setMailSession(Session.getDefaultInstance(new Properties()))

  def from = commonsmail.getFromAddress.toString
  def to = commonsmail.getToAddresses.map(_.toString)
  def cc = commonsmail.getCcAddresses.map(_.toString)
  def bcc = commonsmail.getBccAddresses.map(_.toString)
  //suppose we have only text messages
  def body = {
    commonsmail.buildMimeMessage()
    commonsmail.getMimeMessage.getContent match {
      case content: String => content.split("\r\n")
      case content: Multipart => (for (i <- 1 to content.getCount) yield content.getBodyPart(i).getContent match{
        case part: String => part.split("\r\n")
        case _ => Array[String]()
      }).flatten
      case _ => Seq[String]()
    }
  }
}
*/