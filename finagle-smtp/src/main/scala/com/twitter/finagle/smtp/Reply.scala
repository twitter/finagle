package com.twitter.finagle.smtp

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
  val OK                          = 250
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

import ReplyCode._

private[smtp] trait Reply extends UnspecifiedReply

trait Error extends Exception with Reply

case class InvalidReply(content: String) extends Error {
  val code = INVALID_REPLY_CODE
  val info = content
}
case class UnknownReplyCodeError(override val code: Int, info: String) extends Error

/*Differentiating by success*/
trait PositiveCompletionReply extends Reply
trait PositiveIntermediateReply extends Reply
trait TransientNegativeCompletionReply extends Error
trait PermanentNegativeCompletionReply extends Error

/*Differentiating by context*/
trait SyntaxReply extends Reply
trait InformationReply extends Reply
trait ConnectionsReply extends Reply
trait MailSystemReply extends Reply

/*Replies by groups*/
trait SyntaxErrorReply extends PermanentNegativeCompletionReply with SyntaxReply
trait SystemInfoReply extends PositiveCompletionReply with InformationReply
trait ServiceInfoReply extends PositiveCompletionReply with ConnectionsReply
trait NotAvailableReply extends TransientNegativeCompletionReply with ConnectionsReply
trait MailOkReply extends PositiveCompletionReply with MailSystemReply
trait MailIntermediateReply extends PositiveIntermediateReply with MailSystemReply
trait MailErrorReply extends TransientNegativeCompletionReply with MailSystemReply
trait ActionErrorReply extends TransientNegativeCompletionReply with MailSystemReply


/*Syntax errors*/
case class SyntaxError(info: String) extends SyntaxErrorReply {
  val code = SYNTAX_ERROR
}

case class ArgumentSyntaxError(info: String) extends SyntaxErrorReply {
  val code = ARGUMENT_SYNTAX_ERROR
}
case class CommandNotImplemented(info: String) extends SyntaxErrorReply {
  val code = COMMAND_NOT_IMPLEMENTED
}
case class BadCommandSequence(info: String) extends SyntaxErrorReply {
  val code = BAD_COMMAND_SEQUENCE
}
case class ParameterNotImplemented(info: String) extends SyntaxErrorReply {
  val code = PARAMETER_NOT_IMPLEMENTED
}

/*System information*/
case class SystemStatus(info: String) extends SystemInfoReply {
  val code = SYSTEM_STATUS
}
case class Help(info: String) extends SystemInfoReply {
  val code = HELP
}

/*Service information*/
case class ServiceReady(info: String) extends ServiceInfoReply {
  val code = SERVICE_READY
}
case class ClosingTransmission(info: String) extends ServiceInfoReply {
  val code = CLOSING_TRANSMISSION
}
case class ServiceNotAvailable(info: String) extends NotAvailableReply {
  val code = SERVICE_NOT_AVAILABLE
}

/*Mail system successes*/
case class OKReply(info: String) extends MailOkReply {
  val code = OK
}

case class TempUserNotLocal(info: String) extends MailOkReply {
  val code = TEMP_USER_NOT_LOCAL
}
case class TempUserNotVerified(info: String) extends MailOkReply {
  val code = TEMP_USER_NOT_VERIFIED
}
case class StartInput(info: String) extends MailIntermediateReply {
  val code = START_INPUT
}

/*Mail system errors*/
case class MailboxUnavailableError(info: String) extends MailErrorReply {
  val code = MAILBOX_UNAVAILABLE_ERROR
}
case class UserNotLocalError(info: String) extends MailErrorReply {
  val code = USER_NOT_LOCAL_ERROR
}
case class InsufficientStorageError(info: String) extends MailErrorReply {
  val code = INSUFFICIENT_STORAGE_ERROR
}
case class InvalidMailboxName(info: String) extends MailErrorReply {
  val code = INVALID_MAILBOX_NAME
}
case class TransactionFailed(info: String) extends MailErrorReply {
  val code = TRANSACTION_FAILED
}
case class AddressNotRecognized(info: String) extends MailErrorReply {
  val code = ADDRESS_NOT_RECOGNIZED
}

/*Errors in performing requested action*/
case class TempMailboxUnavailable(info: String) extends ActionErrorReply {
  val code = TEMP_MAILBOX_UNAVAILABLE
}
case class ProcessingError(info: String) extends ActionErrorReply {
  val code = PROCESSING_ERROR
}
case class TempInsufficientStorage(info: String) extends ActionErrorReply {
  val code = TEMP_INSUFFICIENT_STORAGE
}
case class ParamsAccommodationError(info: String) extends ActionErrorReply {
  val code = PARAMS_ACCOMODATION_ERROR
}
