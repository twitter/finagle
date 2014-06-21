package com.twitter.finagle.smtp.reply

trait UnspecifiedReply {
  val code: Int
  val info: String

  val isMultiline: Boolean = false
  val lines: Seq[String] = Seq(info)
}

case class MultilinePart(code: Int, info: String) extends UnspecifiedReply

object ReplyCode {

  val SYSTEM_STATUS               = 211
  val HELP                        = 214
  val SERVICE_READY               = 220
  val CLOSING_TRANSMISSION        = 221
  val OKReply                     = 250
  val TEMP_USER_NOT_LOCAL         = 251
  val TEMP_USER_NOT_VERIFIED      = 252
  val START_INPUT                 = 354
  val SERVICE_NOT_AVAILABLE       = 421
  val TEMP_MAILBOX_UNAVAILABLE    = 450
  val PROCESSING_ERROR            = 451
  val TEMP_INSUFFICIENT_STORAGE   = 452
  val PARAMS_ACCOMODATION_ERROR   = 455
  val SYNTAX_ERROR                = 500
  val ARGUMENT_SYNTAX_ERROR       = 501
  val COMMAND_NOT_IMPLEMENTED     = 502
  val BAD_COMMAND_SEQUENCE        = 503
  val PARAMETER_NOT_IMPLEMENTED   = 504
  val MAILBOX_UNAVAILABLE_ERROR   = 550
  val USER_NOT_LOCAL_ERROR        = 551
  val INSUFFICIENT_STORAGE_ERROR  = 552
  val INVALID_MAILBOX_NAME        = 553
  val TRANSACTION_FAILED          = 554
  val ADDRESS_NOT_RECOGNIZED      = 555

  val INVALID_REPLY_CODE          = -1
}

trait Reply extends UnspecifiedReply
