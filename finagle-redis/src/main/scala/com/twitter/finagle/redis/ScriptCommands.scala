package com.twitter.finagle.redis

import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.ReplyFormat
import com.twitter.io.Buf
import com.twitter.util.Future

private[redis] trait ScriptCommands { self: BaseClient =>
  private[redis] val filterReply: PartialFunction[Reply, Future[Reply]] = {
    case ErrorReply(message) => Future.exception(new ServerError(message))
    case reply: Reply => Future.value(reply)
  }

  /**
   * Executes EVAL command returns the raw redis-server [[Reply]].
   *
   * User may convert this [[Reply]] to type T by calling their [[Scripts.castReply[T](reply)]],
   * or "reply.cast[T]" which is an implicit conversion through [[Scripts.CastableReply]] helper class.
   *
   * An idiomatic usage:
   * {{{
   *   import Scripts._
   *   client.eval(script, keys, argv).map { _.cast[Long] }
   * }}}
   * returns a Future[Long], and is equivalent to
   * {{{ client.eval(script, keys, argv).map(Scripts.castReply[Long]) }}}
   *
   * @param script the script to execute
   * @param keys the redis keys that the script may have access to
   * @param argv the array of arguments that the script takes
   * @return redis-server [[Reply]]
   */
  def eval(script: Buf, keys: Seq[Buf], argv: Seq[Buf]): Future[Reply] =
    doRequest(Eval(script, keys, argv))(filterReply)

  /**
   * Executes EVALSHA command and returns the raw redis-server [[Reply]].
   *
   * Similar to [[eval]], but takes "sha" (SHA-1 digest of a script) as the first parameter.
   * If the script cache on redis-server does not contain a script whose SHA-1 digest is "sha",
   * this will return [[ErrorReply(message)]] where message starts with "NOSCRIPT".
   *
   * @param sha SHA-1 digest of the script to execute
   * @param keys the redis keys that the script may have access to
   * @param argv the array of arguments that the script takes
   * @return redis-server [[Reply]]
   */
  def evalSha(sha: Buf, keys: Seq[Buf], argv: Seq[Buf]): Future[Reply] =
    doRequest(EvalSha(sha, keys, argv))(filterReply)

  /**
   * Executes EVALSHA command, and if redis-server complains about NOSCRIPT, execute EVAL with fallback "script" instead.
   *
   * Similar to [[evalSha]], but this takes a fallback "script" parameter in addition to "sha",
   * so that any [[ErrorReply("NOSCRIPT...")]] will be silently caught and a corresponding EVAL command will be retried.
   *
   * @param sha SHA-1 digest of the script to execute
   * @param script the fallback script. User must guarantee that {{{sha == SHA1(script)}}},
   *               where "SHA1" computes the SHA-1 digest (as a HEX string) of script.
   * @param keys the redis keys that the script may have access to
   * @param argv the array of arguments that the script takes
   * @return redis-server [[Reply]]
   */
  def evalSha(sha: Buf, script: Buf, keys: Seq[Buf], argv: Seq[Buf]): Future[Reply] = {
    doRequest(EvalSha(sha, keys, argv)) {
      case ErrorReply(message) =>
        if (message.startsWith("NOSCRIPT"))
          eval(script, keys, argv)
        else
          Future.exception(new ServerError(message))
      case reply: Reply =>
        Future.value(reply)
    }
  }

  /**
   * Returns whether each sha1 digest in "digests" indicates some valid script on redis-server script cache.
   */
  def scriptExists(digests: Buf*): Future[Seq[Boolean]] = {
    doRequest(ScriptExists(digests)) {
      case MBulkReply(message) =>
        Future.value(message.map {
          case IntegerReply(n) => n != 0
          case _ => false
        })
    }
  }

  /**
   * Flushes the script cache on redis-server.
   */
  def scriptFlush(): Future[Unit] = {
    doRequest(ScriptFlush) {
      // SCRIPT FLUSH must return a simple string reply indicating OK status
      case StatusReply(_) => Future.Unit
    }
  }

  /**
   * Loads a script to redis-server script cache, and returns its SHA-1 digest as a HEX string.
   *
   * Note that the SHA-1 digest of "script" (as a HEX string) can also be calculated locally,
   * and it does equal what will be returned by redis-server,
   * but this call, once succeeds, has the side effect of loading the script to the cache on redis-server.
   */
  def scriptLoad(script: Buf): Future[Buf] = {
    doRequest(ScriptLoad(script)) {
      // SCRIPT LOAD must return a non-empty bulk string reply indicating the SHA1 digest of the loaded script
      case BulkReply(message) => Future.value(message)
    }
  }
}

object ScriptCommands {

  /**
   * Converts a redis-server [[Reply]] to type [[T]].
   *
   * The conversion is best-effort, and if it fails, a [[ReplyCastFailure]] will be thrown.
   *
   * Currently supported target types:
   *   Unit
   *   Long
   *   Boolean
   *   Buf
   *   Seq[Buf]
   */
  def castReply[T](reply: Reply)(implicit ev: ReplyConverter[T]): T =
    ev.cast(reply)

  implicit class CastableReply(val reply: Reply) extends AnyVal {
    def cast[T]()(implicit ev: ReplyConverter[T]): T =
      ev.cast(reply)
  }

  /**
   * type class to help converting [[Reply]] to specific type [[T]]
   */
  trait ReplyConverter[T] {
    def cast(reply: Reply): T
  }

  object ReplyConverter {

    implicit val unit = new ReplyConverter[Unit] {
      override def cast(reply: Reply): Unit = reply match {
        case StatusReply(_) | EmptyBulkReply => ()
        case r => Future.exception(ReplyCastFailure("Error casting " + r + " to Unit"))
      }
    }

    implicit val long = new ReplyConverter[Long] {
      override def cast(reply: Reply): Long = reply match {
        case IntegerReply(n) => n
        case r => throw new ReplyCastFailure("Error casting " + r + " to Long")
      }
    }

    implicit val bool = new ReplyConverter[Boolean] {
      override def cast(reply: Reply): Boolean = reply match {
        case IntegerReply(n) => (n == 1)
        case EmptyBulkReply => false
        case StatusReply(_) => true
        case r => throw new ReplyCastFailure("Error casting " + r + " to Boolean")
      }
    }

    implicit val buffer = new ReplyConverter[Buf] {
      override def cast(reply: Reply): Buf = reply match {
        case BulkReply(message) => message
        case EmptyBulkReply => Buf.Empty
        case r => throw new ReplyCastFailure("Error casting " + r + " to Buf")
      }
    }

    implicit val buffers = new ReplyConverter[Seq[Buf]] {
      override def cast(reply: Reply): Seq[Buf] = reply match {
        case MBulkReply(messages) => ReplyFormat.toBuf(messages)
        case EmptyMBulkReply => Nil
        case r => throw new ReplyCastFailure("Error casting " + r + " to Seq[Buf]")
      }
    }
  }
}
