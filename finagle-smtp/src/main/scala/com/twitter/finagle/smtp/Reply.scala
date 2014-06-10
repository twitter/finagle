package com.twitter.finagle.smtp

trait UnspecifiedReply {
  val code: Int
  val info: String
}

object ReplyCode {
  val REPLY_TYPE_SPECIFIED = 1
  val INVALID_REPLY_CODE = -1
}

trait Reply extends UnspecifiedReply {
  val code = ReplyCode.REPLY_TYPE_SPECIFIED
}

trait Error extends Exception with Reply {
  override val code = ReplyCode.INVALID_REPLY_CODE
}

//used in cases when you need just to return something
case object EmptyReply extends Reply {
  override val code = ReplyCode.INVALID_REPLY_CODE
  val info = ""
}

case class InvalidReply(content: String) extends Error {
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
case class SyntaxError(info: String) extends SyntaxErrorReply
case class ArgumentSyntaxError(info: String) extends SyntaxErrorReply
case class CommandNotImplemented(info: String) extends SyntaxErrorReply
case class BadCommandSequence(info: String) extends SyntaxErrorReply
case class ParameterNotImplemented(info: String) extends SyntaxErrorReply

/*System information*/
case class SystemStatus(info: String) extends SystemInfoReply
case class Help(info: String) extends SystemInfoReply

/*Service information*/
case class ServiceReady(info: String) extends ServiceInfoReply
case class ClosingTransmission(info: String) extends ServiceInfoReply
case class ServiceNotAvailable(info: String) extends NotAvailableReply

/*Mail system successes*/
case class OK(info: String) extends MailOkReply

//for greeting with extensions
case class Extension(info: String) extends MailOkReply

case class AvailableExtensions(info: String, ext: Seq[Extension], last: OK) extends MailOkReply

//Greeting contains ServiceReady message and whatever is received next
case class Greeting(info: String, ext: Reply) extends ServiceInfoReply

case class TempUserNotLocal(info: String) extends MailOkReply
case class TempUserNotVerified(info: String) extends MailOkReply
case class StartInput(info: String) extends MailIntermediateReply

/*Mail system errors*/
case class MailboxUnavailableError(info: String) extends MailErrorReply
case class UserNotLocalError(info: String) extends MailErrorReply
case class InsufficientStorageError(info: String) extends MailErrorReply
case class InvalidMailboxName(info: String) extends MailErrorReply
case class TransactionFailed(info: String) extends MailErrorReply
case class AddressNotRecognized(info: String) extends MailErrorReply

/*Errors in performing requested action*/
case class TempMailboxUnavailable(info: String) extends ActionErrorReply
case class ProcessingError(info: String) extends ActionErrorReply
case class TempInsufficientStorage(info: String) extends ActionErrorReply
case class ParamsAccommodationError(info: String) extends ActionErrorReply
