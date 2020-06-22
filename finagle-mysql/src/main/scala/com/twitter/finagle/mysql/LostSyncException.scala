package com.twitter.finagle.mysql

import com.twitter.util.{Future, Throw, Try}

/**
 * A [[LostSyncException]] indicates that this finagle-mysql client and the MySQL server are
 * no longer able to properly communicate, as there has been a failure to decode a message
 * from the server or data has become corrupted in transmission. It is a fatal error and the
 * communication with the server must be closed, then reopened, and renegotiated.
 */
final case class LostSyncException(underlying: Throwable) extends RuntimeException(underlying) {
  override def getMessage: String = underlying.toString
  override def getStackTrace: Array[StackTraceElement] = underlying.getStackTrace
}

// The companion object needs to be public for folks to be able to match on the
// `LostSyncException`.
object LostSyncException {

  private[mysql] val AsException: LostSyncException = LostSyncException(new Throwable)
  private[mysql] val AsFuture: Future[Nothing] = Future.exception(AsException)

  /**
   * Wrap a Try[T] into a Future[T]. This is useful for
   * transforming decoded results into futures. Any Throw
   * is assumed to be a failure to decode and thus a synchronization
   * error (or corrupt data) between the client and server.
   */
  private[mysql] def const[T](result: Try[T]): Future[T] =
    Future.const(result.rescue { case exc => Throw(LostSyncException(exc)) })

}
