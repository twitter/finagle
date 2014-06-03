package com.twitter.finagle.smtp

trait Reply {
  val firstDigit: Char
  val secondDigit: Char
  val thirdDigit: Char
  val info: String
  def getCode: Int =  (StringBuilder.newBuilder + firstDigit + secondDigit + thirdDigit).toInt
  val isValid = true
  override def toString = StringBuilder.newBuilder + firstDigit + secondDigit + thirdDigit + " " + info
}

trait Error extends Exception with Reply

case class UnknownReply(info: String) extends Error {
  val firstDigit = '0'
  val secondDigit = '0'
  val thirdDigit = '0'
  override val isValid = false
}
case class UnknownReplyCodeError(firstDigit: Char, secondDigit: Char, thirdDigit: Char, info: String) extends Error {
  override val isValid = false
}

/*Differentiating by first digit*/
trait PositiveCompletionReply extends Reply {
  val firstDigit = '2'
}

trait PositiveIntermediateReply extends Reply {
  val firstDigit = '3'
}

trait TransientNegativeCompletionReply extends Error {
  val firstDigit = '4'
}

trait PermanentNegativeCompletionReply extends Error {
  val firstDigit = '5'
}

/*Differentiating by second digit*/
trait SyntaxReply extends Reply {
  val secondDigit = '0'
}

trait InformationReply extends Reply {
  val secondDigit = '1'
}

trait ConnectionsReply extends Reply {
  val secondDigit = '2'
}

trait MailSystemReply extends Reply {
  val secondDigit = '5'
}

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
case class SyntaxError(inf: String) extends SyntaxErrorReply {
  val info = inf
  val thirdDigit = '0'
}
case class ArgumentSyntaxError(inf: String) extends SyntaxErrorReply {
  val info = inf
  val thirdDigit = '1'
}
case class CommandNotImplemented(inf: String) extends SyntaxErrorReply {
  val info = inf
  val thirdDigit = '2'
}
case class BadCommandSequence(inf: String) extends SyntaxErrorReply {
  val info = inf
  val thirdDigit = '3'
}
case class ParameterNotImplemented(inf: String) extends SyntaxErrorReply {
  val info = inf
  val thirdDigit = '4'
}

/*System information*/
case class SystemStatus(inf: String) extends SystemInfoReply {
  val info = inf
  val thirdDigit = '1'
}
case class Help(inf: String) extends SystemInfoReply {
  val info = inf
  val thirdDigit = '4'
}

/*Service information*/
case class ServiceReady(inf: String) extends ServiceInfoReply {
  val info = inf
  val thirdDigit = '0'
}
case class ClosingTransmission(inf: String) extends ServiceInfoReply {
  val info = inf
  val thirdDigit = '1'
}
case class ServiceNotAvailable(inf: String) extends NotAvailableReply {
  val info = inf
  val thirdDigit = '1'
}

/*Mail system successes*/
case class OK(inf: String) extends MailOkReply {
  val info = inf
  val thirdDigit = '0'
}
case class TempUserNotLocal(inf: String) extends MailOkReply {
  val info = inf
  val thirdDigit = '1'
}
case class TempUserNotVerified(inf: String) extends MailOkReply {
  val info = inf
  val thirdDigit = '2'
}
case class StartInput(inf: String) extends MailIntermediateReply {
  val info = inf
  val thirdDigit = '4'
}

/*Mail system errors*/
case class MailboxUnavailableError(inf: String) extends MailErrorReply {
  val info = inf
  val thirdDigit = '0'
}
case class UserNotLocalError(inf: String) extends MailErrorReply {
  val info = inf
  val thirdDigit = '1'
}
case class InsufficientStorageError(inf: String) extends MailErrorReply {
  val info = inf
  val thirdDigit = '2'
}
case class InvalidMailboxName(inf: String) extends MailErrorReply {
  val info = inf
  val thirdDigit = '3'
}
case class TransactionFailed(inf: String) extends MailErrorReply {
  val info = inf
  val thirdDigit = '4'
}
case class AddressNotRecognized(inf: String) extends MailErrorReply {
  val info = inf
  val thirdDigit = '5'
}

/*Errors in performing requested action*/
case class TempMailboxUnavailable(inf: String) extends ActionErrorReply {
  val info = inf
  val thirdDigit = '0'
}
case class ProcessingError(inf: String) extends ActionErrorReply {
  val info = inf
  val thirdDigit = '1'
}
case class TempInsufficientStorage(inf: String) extends ActionErrorReply {
  val info = inf
  val thirdDigit = '2'
}
case class ParamsAccommodationError(inf: String) extends ActionErrorReply {
  val info = inf
  val thirdDigit = '5'
}
