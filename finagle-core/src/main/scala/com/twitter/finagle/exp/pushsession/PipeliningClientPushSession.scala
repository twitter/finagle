package com.twitter.finagle.exp.pushsession

import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.{ChannelClosedException, Failure, Service, Status}
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
private[finagle] final class PipeliningClientPushSession[In, Out](
    handle: PushChannelHandle[In, Out],
    statsReceiver: StatsReceiver,
    stallTimeout: Duration,
    timer: Timer)
  extends PushSession[In, Out](handle) { self =>

  private[this] val logger = Logger.get
  private[this] val queue = new java.util.ArrayDeque[Promise[In]]()
  private[this] var stalled: Boolean = false   // used only within SerialExecutor

  // volatile because accessed outside of SerialExecutor
  @volatile private[this] var queueSize: Int = 0  // avoids synchronization on `queue`
  @volatile private[this] var running: Boolean = true

  // exposed for testing
  private[pushsession] def getQueueSize: Int = queueSize

  handle.onClose.respond { result =>
    if (running) handle.serialExecutor.execute(new Runnable {
      def run(): Unit = result match {
        case Return(_) => handleShutdown(None)
        case Throw(t) => handleShutdown(Some(t))
      }
    })
  }

  def receive(message: In): Unit = if (running) {
    val p = queue.poll()
    if (p != null) {
      queueSize -= 1
      p.updateIfEmpty(Return(message))
    }
    else
      handleShutdown(
        Some(new IllegalStateException("Received response with no corresponding request: " + message))
      )
  }

  def status: Status = {
    if (!running) Status.Closed
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
                  if (!stalled) {
                    stalled = true
                    val addr = handle.remoteAddress
                    logger.warning(
                      s"pipelined connection stalled with $queueSize items, talking to $addr"
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
    Failure(s"The connection pipeline could not make progress in $timeout", Failure.Interrupted)

  // All shutdown pathways should funnel through this method
  private[this] def handleShutdown(cause: Option[Throwable]): Unit =
    if (running) {
      running = false
      cause.foreach(logger.info(_, "Session closing with exception"))
      close()
      val exc = cause.getOrElse(new ChannelClosedException(handle.remoteAddress))

      // Clear the queue.
      while (!queue.isEmpty) {
        queue.poll().updateIfEmpty(Throw(exc))
        queueSize -=1
      }
    }

  private[this] def handleDispatch(request: Out, p: Promise[In]): Unit = {
    if (!running) p.setException(new ChannelClosedException(handle.remoteAddress))
    else {
      queue.offer(p)
      queueSize +=1
      handle.sendAndForget(request)
    }
  }
}
