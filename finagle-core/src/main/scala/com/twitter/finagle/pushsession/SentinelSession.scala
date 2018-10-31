package com.twitter.finagle.pushsession
import com.twitter.finagle.Status
import com.twitter.util.{Future, Time}
import com.twitter.logging.Logger

private[finagle] object SentinelSession {

  private[this] val log = Logger.get

  /** Build a sentinel `PushSession`
   *
   * The resulting `PushSession` is only a place holder, a resource aware version
   * of `null`, that is is never intended to receive events, and logs an error if
   * it does while shutting down the pipeline.
   */
  def apply[In, Out](handle: PushChannelHandle[In, Out]): PushSession[In, Out] =
    new SentinelSession[In, Out](handle)

  // The implementation
  private final class SentinelSession[In, Out](handle: PushChannelHandle[In, Out])
      extends PushSession[In, Out](handle) {

    // Should never be called if this is used correctly, but if it does, get loud and shut things down.
    def receive(message: In): Unit = {
      val msg = s"${this.getClass.getSimpleName} received unexpected message: " +
        s"${message.getClass.getSimpleName}. This is a bug and should be reported as we may be " +
        "leaking resources!"

      val ex = new IllegalStateException(msg)
      log.error(ex, msg)
      close()
    }

    def status: Status = handle.status

    def close(deadline: Time): Future[Unit] = handle.close(deadline)
  }
}
