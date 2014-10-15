package com.twitter.finagle.smtp.reply

import ReplyCode._

/* Syntax errors */

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

/* System information */

case class SystemStatus(info: String) extends SystemInfoReply {
  val code = SYSTEM_STATUS
}
case class Help(info: String) extends SystemInfoReply {
  val code = HELP
}

/* Service information */

case class ServiceReady(domain: String, info: String) extends ServiceInfoReply {
  val code = SERVICE_READY
}
case class ClosingTransmission(info: String) extends ServiceInfoReply {
  val code = CLOSING_TRANSMISSION
}
case class ServiceNotAvailable(info: String) extends NotAvailableReply {
  val code = SERVICE_NOT_AVAILABLE
}

/* Mail system successes */

case class OK(info: String) extends MailOkReply {
  val code = OK_REPLY
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

/* Mail system errors */

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

/* Errors in performing requested action */

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