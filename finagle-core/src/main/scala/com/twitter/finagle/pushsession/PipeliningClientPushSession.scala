package com.twitter.finagle.pushsession

import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.Failure
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Service
import com.twitter.finagle.Status
import com.twitter.logging.Logger
import com.twitter.util._

/**
 * A Pipelined [[PushSession]].
 *
 * This assumes servers will respect normal pipelining semantics, and that
 * replies will be sent in the same order as requests were sent.
 *
 * Because many requests might be sharing the same push channel,
 * [[com.twitter.util.Future Futures]] returned by PipeliningClientPushSession#apply
 * are masked, and will only propagate the interrupt if the Future doesn't
 * return after a configurable amount of time after the interruption.
 * This ensures that interrupting a Future in one request won't change the
 * result of another request unless the connection is stuck, and does not
 * look like it will make progress. Use `stallTimeout` to configure this timeout.
 */
final class PipeliningClientPushSession[In, Out](
  handle: PushChannelHandle[In, Out],
  stallTimeout: Duration,
  timer: Timer)
    extends PushSession[In, Out](handle) { self =>

  private[this] val logger = Logger.get

  // used only within SerialExecutor
  private[this] val h_queue = new java.util.ArrayDeque[Promise[In]]()
  private[this] var h_stalled: Boolean = false
  // These are marked volatile because they are read outside of SerialExecutor but
  // are only modified from within the serial executor.
  @volatile private[this] var h_queueSize: Int = 0 // avoids synchronization on `queue`
  @volatile private[this] var h_running: Boolean = true

  // exposed for testing
  private[pushsession] def getQueueSize: Int = h_queueSize

  handle.onClose.respond { result =>
    if (h_running) handle.serialExecutor.execute(new Runnable {
      def run(): Unit = result match {
        case Return(_) => handleShutdown(None)
        case Throw(t) => handleShutdown(Some(t))
      }
    })
  }

  def receive(message: In): Unit = if (h_running) {
    val p = h_queue.poll()
    if (p != null) {
      h_queueSize -= 1
      p.updateIfEmpty(Return(message))
    } else
      handleShutdown(
        Some(
          new IllegalStateException("Received response with no corresponding request: " + message)
        )
      )
  }

  def status: Status = {
    if (!h_running) Status.Closed
    else handle.status
  }

  def close(deadline: Time): Future[Unit] = handle.close(deadline)

  def toService: Service[Out, In] = new Service[Out, In] {
    def apply(request: Out): Future[In] = {
      val p = Promise[In]
      p.setInterruptHandler {
        case _: Throwable =>
          timer.schedule(Time.now + stallTimeout) {
            handle.serialExecutor.execute(new Runnable {
              def run(): Unit = {
                val exc = stalledPipelineException(stallTimeout)
                if (p.updateIfEmpty(Throw(exc))) {
                  if (!h_stalled) {
                    h_stalled = true
                    val addr = handle.remoteAddress
                    logger.warning(
                      s"pipelined connection stalled with $h_queueSize items, talking to $addr"
                    )
                  }
                  handleShutdown(Some(exc))
                }
              }
            })
          }
      }
      handle.serialExecutor.execute(new Runnable { def run(): Unit = handleDispatch(request, p) })
      p
    }

    override def close(deadline: Time): Future[Unit] = self.close(deadline)

    override def status: Status = self.status
  }

  private[this] def stalledPipelineException(timeout: Duration) =
    Failure(
      s"The connection pipeline could not make progress in $timeout",
      FailureFlags.Interrupted
    )

  // All shutdown pathways should funnel through this method
  private[this] def handleShutdown(cause: Option[Throwable]): Unit =
    if (h_running) {
      h_running = false
      cause.foreach(logger.info(_, "Session closing with exception"))
      close()
      val exc = cause.getOrElse(new ChannelClosedException(handle.remoteAddress))

      // Clear the queue.
      while (!h_queue.isEmpty) {
        h_queue.poll().updateIfEmpty(Throw(exc))
        h_queueSize -= 1
      }
    }

  private[this] def handleDispatch(request: Out, p: Promise[In]): Unit = {
    if (!h_running) p.setException(new ChannelClosedException(handle.remoteAddress))
    else {
      h_queue.offer(p)
      h_queueSize += 1
      handle.sendAndForget(request)
    }
  }
}
