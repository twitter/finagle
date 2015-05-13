package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.util.Future
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}

trait Scripts { self: BaseClient =>
  val handleUnit: PartialFunction[Reply, Future[Unit]] = {
    case StatusReply(_) | EmptyBulkReply() => Future.Unit
  }

  val handleLong: PartialFunction[Reply, Future[Long]] = {
    case IntegerReply(n) => Future.value(n)
  }

  val handleBool: PartialFunction[Reply, Future[Boolean]] = {
    case IntegerReply(n)  => Future.value(n == 1)
    case EmptyBulkReply() => Future.False
    case StatusReply(_)   => Future.True
  }

  val handleBuffer: PartialFunction[Reply, Future[ChannelBuffer]] = {
    case BulkReply(message) => Future.value(message)
    case EmptyBulkReply() => Future.value(ChannelBuffers.EMPTY_BUFFER)
  }

  val handleBuffers: PartialFunction[Reply, Future[Seq[ChannelBuffer]]] = {
    case MBulkReply(messages) => Future.value(ReplyFormat.toChannelBuffers(messages))
    case EmptyMBulkReply()    => Future.Nil
  }

  def retryOnNoScript[T](retry: () => T): PartialFunction[Reply, T] = {
    // NOTE: we cannot make retry call-by-value.
    // if we make retry call-by-value, the evalXXX() request will happen immediately
    // when we construct the partial function, thus evalXXX() will be executed before evalShaXXX().
    // NOTE: so we need call-by-need. but if we use "retry: => T",
    // when we debug the program, the debugger will force evaluate retry EVERY TIME
    // when retryOnNoScript is the current frame, resulting in confusion,
    // especially when retry has side-effects.
    // so we make retry: () => T an explicity function to prevent the debugger working too eagerly
    case ErrorReply(message) if message.startsWith("NOSCRIPT") =>
      retry()
  }

  // NOTE: because there is no easy way of telling the return type of the passed script,
  // we rely on the user to tell us the return type.
  // Thus the different eval* and evalSha* methods correspond to different expected return types.
  // TODO: currently if the actual reply is different from the told return type,
  // an IllegalStateException will be thrown. We may want to use a more specific exception for this case.
  def evalUnit(script: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer]): Future[Unit] = {
    doRequest(Eval(script, keys, argv))(handleUnit)
  }

  // NOTE: all evalSha* methods take an additional script: Option[ChannelBuffer] parameter,
  // which is a "fallback" original script if the user wants to provide.
  // In case the redis-server replies with "NOSCRIPT" error, evalSha* will retry with corresponding eval*
  // given that the user has provided Some(script).
  def evalShaUnit(digest: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer],
                  fallbackScript: Option[ChannelBuffer] = None): Future[Unit] = {
    doRequest (EvalSha(digest, keys, argv) ) (fallbackScript match {
      case Some(script) =>
        handleUnit orElse retryOnNoScript(() => evalUnit(script, keys, argv))
        // NOTE: the retryOnNoScript(...) is equivalent to the code snippet below:
//          {
//            case ErrorReply(message) if message.contains("NOSCRIPT") =>
//              evalUnit(s, keys, argv)
//          }
      case _ =>
         handleUnit
    })
  }

  def evalLong(script: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer]): Future[Long] = {
    doRequest(Eval(script, keys, argv))(handleLong)
  }

  def evalShaLong(digest: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer],
                  fallbackScript: Option[ChannelBuffer] = None): Future[Long] = {
    doRequest (EvalSha(digest, keys, argv) ) (fallbackScript match {
      case Some(script) =>
        handleLong orElse retryOnNoScript(() => evalLong(script, keys, argv))
      case _ =>
        handleLong
    })
  }

  def evalBool(script: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer]): Future[Boolean] = {
    doRequest(Eval(script, keys, argv))(handleBool)
  }

  def evalShaBool(digest: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer],
                  fallbackScript: Option[ChannelBuffer] = None): Future[Boolean] = {
    doRequest (EvalSha(digest, keys, argv) ) (fallbackScript match {
      case Some(script) =>
        handleBool orElse retryOnNoScript(() => evalBool(script, keys, argv))
      case _ =>
        handleBool
    })
  }

  def evalBuffer(script: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer]): Future[ChannelBuffer] = {
    doRequest(Eval(script, keys, argv))(handleBuffer)
  }

  def evalShaBuffer(digest: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer],
                  fallbackScript: Option[ChannelBuffer] = None): Future[ChannelBuffer] = {
    doRequest (EvalSha(digest, keys, argv) ) (fallbackScript match {
      case Some(script) =>
        handleBuffer orElse retryOnNoScript(() => evalBuffer(script, keys, argv))
      case _ =>
        handleBuffer
    })
  }

  // TODO this way of handling MBulkReply flattens the tree-like structure, and loses some information
  def evalBuffers(script: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer]): Future[Seq[ChannelBuffer]] = {
    doRequest(Eval(script, keys, argv))(handleBuffers)
  }

  def evalShaBuffers(digest: ChannelBuffer, keys: Seq[ChannelBuffer], argv: Seq[ChannelBuffer],
                  fallbackScript: Option[ChannelBuffer] = None): Future[Seq[ChannelBuffer]] = {
    doRequest (EvalSha(digest, keys, argv) ) (fallbackScript match {
      case Some(script) =>
        handleBuffers orElse retryOnNoScript(() => evalBuffers(script, keys, argv))
      case _ =>
        handleBuffers
    })
  }

  def scriptExists(digests: ChannelBuffer*): Future[Seq[Boolean]] = {
    doRequest(ScriptExists(digests)) {
      case MBulkReply(message) => Future.value(message map {
        case IntegerReply(n) => n != 0
        case _ => false
      })
    }
  }

  def scriptFlush(): Future[Unit] = {
    doRequest(ScriptFlush)(handleUnit)
  }

  def scriptLoad(script: ChannelBuffer): Future[ChannelBuffer] = {
    doRequest(ScriptLoad(script))(handleBuffer)
  }
}
