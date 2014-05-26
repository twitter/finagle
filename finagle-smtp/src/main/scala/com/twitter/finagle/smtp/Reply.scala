package com.twitter.finagle.smtp

trait Reply {
  val firstDigit: Char
  val secondDigit: Char
  val thirdDigit: Char
  val info: String
  def getCode: Int =  (StringBuilder.newBuilder + firstDigit + secondDigit + thirdDigit).toInt
}

trait Error extends Exception with Reply

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
case class SyntaxErrorReply(thirdDigit: Char, info: String) extends PermanentNegativeCompletionReply with SyntaxReply
case class SystemInfoReply(thirdDigit: Char, info: String) extends PositiveCompletionReply with InformationReply
case class ServiceInfoReply(thirdDigit: Char, info: String) extends PositiveCompletionReply with ConnectionsReply
case class NotAvailableReply(thirdDigit: Char, info: String) extends TransientNegativeCompletionReply with ConnectionsReply
case class MailOkReply(thirdDigit: Char, info: String) extends PositiveCompletionReply with MailSystemReply
case class MailErrorReply(thirdDigit: Char, info: String) extends TransientNegativeCompletionReply with MailSystemReply
case class ActionErrorReply(thirdDigit: Char, info: String) extends TransientNegativeCompletionReply with MailSystemReply



/*Syntax errors*/
case class SyntaxError(inf: String) extends SyntaxErrorReply('0', inf)
case class ArgumentSyntaxError(inf: String) extends SyntaxErrorReply('1', inf)
case class CommandNotImplemented(inf: String) extends SyntaxErrorReply('2', inf)
case class BadCommandSequence(inf: String) extends SyntaxErrorReply('3', inf)
case class ParameterNotImplemented(inf: String) extends SyntaxErrorReply('4', inf)

/*System information*/
case class SystemStatus(inf: String) extends SystemInfoReply('1', inf)
case class Help(inf: String) extends SystemInfoReply('4', inf)

/*Service information*/
case class ServiceReady(inf: String) extends ServiceInfoReply('0', inf)
case class ClosingTransmission(inf: String) extends ServiceInfoReply('1', inf)
case class ServiceNotAvailable(inf: String) extends NotAvailableReply('1', inf)

/*Mail system successes*/
case class OK(inf: String) extends MailOkReply('0', inf)
case class TempUserNotLocal(inf: String) extends MailOkReply('1', inf)
case class TempUserNotVerified(inf: String) extends MailOkReply('2', inf)

/*Mail system errors*/
case class MailboxUnavailableError(inf: String) extends MailErrorReply('0', inf)
case class UserNotLocalError(inf: String) extends MailErrorReply('1', inf)
case class InsufficientStorageError(inf: String) extends MailErrorReply('2', inf)
case class InvalidMailboxName(inf: String) extends MailErrorReply('3', inf)
case class TransactionFailed(inf: String) extends MailErrorReply('4', inf)
case class AddressNotRecognized(inf: String) extends MailErrorReply('5', inf)

/*Errors in performing requested action*/
case class TempMailboxUnavailable(inf: String) extends ActionErrorReply('0', inf)
case class ProcessingError(inf: String) extends ActionErrorReply('1', inf)
case class TempInsufficientStorage(inf: String) extends ActionErrorReply('2', inf)
case class ParamsAccommodationError(inf: String) extends ActionErrorReply('5', inf)
