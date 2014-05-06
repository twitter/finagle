package com.twitter.finagle.smtp

import javax.mail.internet.InternetAddress
import javax.mail.{Session, Multipart, Message}
import scala.collection.JavaConversions._
import java.util.Properties

//case class Email(from: String, to: String, body: String)

trait EmailMessage {
  def from: String 
  def to: Seq[String]
  def cc: Seq[String]
  def bcc: Seq[String]
  def body: Seq[String]
}

object EmailMessage {
  def apply(_from: String, _to: Seq[String], _cc: Seq[String], _bcc: Seq[String],_body: Seq[String]) = new EmailMessage{
    def from = _from
    def to = _to
    def cc = _cc
    def bcc = _bcc
    def body = _body
  }

  def apply(javamail: javax.mail.internet.MimeMessage) = new JavaMailMessage(javamail)

  def apply(commonsmail: org.apache.commons.mail.Email) = new CommonsMailMessage(commonsmail)
}

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
