package com.twitter.finagle.smtp.reply

trait Error extends Exception with Reply

case class InvalidReply(content: String) extends Error {
  val code = ReplyCode.INVALID_REPLY_CODE
  val info = content
}
case class UnknownReplyCodeError(override val code: Int, info: String) extends Error
