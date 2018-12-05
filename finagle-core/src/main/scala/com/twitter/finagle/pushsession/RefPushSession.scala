package com.twitter.finagle.pushsession

import com.twitter.finagle.Status
import com.twitter.util.{Future, Promise, Time}
import scala.util.control.NonFatal

/**
 * Proxy [[PushSession]] which can update the underlying session.
 *
 * Thread safety considerations
 *
 * - `receive` is only intended to be called from within the serial executor, which is
 *   a general rule in all `PushSession` implementations.
 * - `updateRef` should be called only from within the handle's serial executor. This
 *   is to avoid race conditions between closing the underlying session (which happens
 *   in the serial executor) and replacing it with a new session: if replacing happens
 *   in the same thread, there is no need to worry about broadcasting close events
 *   from the old session to the new one.
 * - `close` and `status` are safe to call from any thread.
 */
final class RefPushSession[In, Out](
  handle: PushChannelHandle[In, Out],
  initial: PushSession[In, Out])
    extends PushSession[In, Out](handle) {

  @volatile
  private[this] var underlying: PushSession[In, Out] = initial

  /**
   * Replaces the current [[PushSession]], directing inbound events to the new session.
   *
   * @note It is unsafe to update the reference from outside of serial executor and
   *       may result in undefined behavior.
   *
   * @note other than no longer receiving inbound messages, the replaced session is still
   *       active and it is the responsibility of the caller to manage the lifecycle of
   *       the replaced session.
   */
  def updateRef(session: PushSession[In, Out]): Unit = {
    underlying = session
  }

  def receive(message: In): Unit = underlying.receive(message)

  def status: Status = underlying.status

  /**
   * Close the underlying session
   *
   * Calls to this method are bounced through the serial-executor to help alleviate
   * race conditions where the underlying session is replaced concurrently with the
   * underlying session being closed.
   */
  def close(deadline: Time): Future[Unit] = {
    val p = Promise[Unit]
    handle.serialExecutor.execute(new Runnable {
      def run(): Unit = {
        val closeF: Future[Unit] =
          try underlying.close(deadline)
          catch { case NonFatal(t) => Future.exception(t) }
        p.become(closeF)
      }
    })
    p
  }
}
