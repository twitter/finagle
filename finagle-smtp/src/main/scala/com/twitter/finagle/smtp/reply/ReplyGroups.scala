package com.twitter.finagle.smtp.reply

/* Differentiating by success */

trait PositiveCompletionReply extends Reply
trait PositiveIntermediateReply extends Reply
trait TransientNegativeCompletionReply extends Error
trait PermanentNegativeCompletionReply extends Error

/* Differentiating by context */

trait SyntaxReply extends Reply
trait InformationReply extends Reply
trait ConnectionsReply extends Reply
trait MailSystemReply extends Reply

/* Replies by groups */

trait SyntaxErrorReply extends PermanentNegativeCompletionReply with SyntaxReply
trait SystemInfoReply extends PositiveCompletionReply with InformationReply
trait ServiceInfoReply extends PositiveCompletionReply with ConnectionsReply
trait NotAvailableReply extends TransientNegativeCompletionReply with ConnectionsReply
trait MailOkReply extends PositiveCompletionReply with MailSystemReply
trait MailIntermediateReply extends PositiveIntermediateReply with MailSystemReply
trait MailErrorReply extends TransientNegativeCompletionReply with MailSystemReply
trait ActionErrorReply extends TransientNegativeCompletionReply with MailSystemReply
