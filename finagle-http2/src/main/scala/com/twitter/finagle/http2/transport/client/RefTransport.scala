package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.Status
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.util.{Future, Time}

/**
 * A [[com.twitter.finagle.transport.Transport]] that proxies another transport,
 * which can be swapped out for other transports.
 *
 * Transport typically represents a single connection, and it's important that
 * this remains true, even though the details of how that connection behaves may
 * change when the underlying transport changes.
 */
class RefTransport[In, Out](underlying: Transport[In, Out])
    extends TransportProxy[In, Out](underlying) {

  @volatile private[this] var mapped = underlying
  private[this] var closeDeadline: Option[Time] = None
  onClose.ensure {
    synchronized {
      if (closeDeadline.isEmpty) {
        closeDeadline = Some(Time.Top)
      }
    }
  }

  def write(msg: In): Future[Unit] = mapped.write(msg)

  def read(): Future[Out] = mapped.read()

  /**
   * Changes the `underlying` transport to be `newTrans` unless it's closing or
   * closed.
   *
   * Note that this changes the original transport that was passed in the
   * constructor, it doesn't act on the last transport the RefTransport was
   * updated to.
   *
   * @return true if it succeeded in changing the underlying transport, false
   *         otherwise
   */
  def update(fn: Transport[In, Out] => Transport[In, Out]): Boolean = synchronized {
    mapped = fn(underlying)
    closeDeadline match {
      case Some(deadline) =>
        mapped.close(deadline)
        false
      case _ => true
    }
  }

  /**
   * Closes the mapped transport, and prevents future updates to the
   * underlying transport.
   */
  override def close(deadline: Time): Future[Unit] = synchronized {
    // prevents further transformations
    closeDeadline = Some(deadline)
    mapped.close(deadline)
  }
  override def status: Status = mapped.status
}
