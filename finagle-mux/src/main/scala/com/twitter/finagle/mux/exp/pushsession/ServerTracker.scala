package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.Service
import com.twitter.finagle.mux.{ClientDiscardedRequestException, Processor, Request, Response}
import com.twitter.finagle.mux.exp.pushsession.ServerTracker._
import com.twitter.finagle.mux.lease.exp.{Lessee, Lessor, nackOnExpiredLease}
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message.{Rdiscarded, Rerr, Tdispatch, Treq}
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util._
import io.netty.util.collection.IntObjectHashMap
import java.net.SocketAddress
import java.util.concurrent.Executor
import scala.collection.JavaConverters._

/**
 * Representation of the Mux server data plane
 *
 * Class responsible for tracking outstanding dispatches, including during draining.
 */
private class ServerTracker(
  serialExecutor: Executor,
  locals: Local.Context,
  service: Service[Request, Response],
  messageWriter: MessageWriter,
  lessor: Lessor,
  statsReceiver: StatsReceiver,
  remoteAddress: SocketAddress
) extends Lessee {

  // We use the netty type because it results in less allocations, but this could
  // just as well be a java Map if we wanted.
  private[this] val dispatches = new IntObjectHashMap[Dispatch]
  private[this] val orphanedTdiscardCounter = statsReceiver.counter("orphaned_tdiscard")

  private[this] val drainedP = Promise[Unit]()
  // All variables expect to enjoy single-threaded manipulation through the executor
  private[this] var state: DrainState = Open
  private[this] var leaseExpiration: Time = Time.Top

  def issue(howLong: Duration): Unit = {
    require(howLong >= Message.Tlease.MinLease)

    // The update of the leaseExpiration and the write of that expiration must be done in
    // one atomic step to ensure that we keep a consistent view of the lease between the
    // client and the server.
    serialExecutor.execute(new Runnable {
      def run(): Unit = {
        leaseExpiration = Time.now + howLong
        messageWriter.write(Message.Tlease(howLong.min(Message.Tlease.MaxLease)))
      }
    })
  }

  /**
   * The number of outstanding dispatches waiting for service response or discard
   */
  def npending(): Int = dispatches.size

  /**
   * Interrupt all outstanding dispatches, raising on their `Future` with an `InterruptedException`
   */
  def interruptOutstandingDispatches(): Unit = {
    // get all the elements, then clear so that the dispatches are definitely cleared
    // before we raise on them, which may cause them to finish with the InterruptedException
    val pending = takeAllDispatches()
    val exc = new InterruptedException
    log.debug(exc, "Interrupting %d dispatches", pending.size)
    pending.foreach(_.response.raise(exc))
  }

  private[this] def canDispatch: Boolean =
    state == Open && !(nackOnExpiredLease() && leaseExpiration <= Time.now)

  /**
   * Provide the current state of the tracker
   */
  def currentState: DrainState = state

  /**
   * Called when receiving a Tdiscarded from the client
   *
   * This call will handle the details of writing a response to the peer, if necessary.
   */
  def discarded(tag: Int, why: String): Unit = {
    dispatches.remove(tag) match {
      case null =>
        if (messageWriter.removeForTag(tag) == MessageWriter.DiscardResult.NotFound) {
          // We either never had a dispatch and the client is misbehaving or
          // or we already sent the response so the tag is freed in servers eyes.
          orphanedTdiscardCounter.incr()
        } else {
          // We had something queued for write so send an `Rdiscarded` to let
          // the peer know that we've aborted the dispatch.
          messageWriter.write(Rdiscarded(tag))
        }

      case dispatch =>
        // We raise on the dispatch and immediately send back a Rdiscarded
        dispatch.response.raise(new ClientDiscardedRequestException(why))
        messageWriter.write(Rdiscarded(tag))

        // By removing a message we may have finished draining, so check.
        checkDrained()
    }
  }

  /**
   * Signals the tracker to begin the draining process
   *
   * When all pending dispatches are drained the `MessageWriter` will be drained as well.
   */
  def drain(): Unit = {
    if (state == Open) {
      state = Draining
      checkDrained()
    }
  }

  /**
   * Signals completion of draining of both the tracker and the `MessageWriter`
   */
  def drained: Future[Unit] = drainedP

  /**
   * Dispatch a Treq message
   */
  def dispatch(request: Treq): Unit = {
    if (canDispatch) doDispatch(request)
    else messageWriter.write(Message.RreqNack(request.tag))
  }

  /**
   * Dispatch a Tdispatch message
   */
  def dispatch(request: Tdispatch): Unit = {
    if (canDispatch) doDispatch(request)
    else messageWriter.write(Message.RdispatchNack(request.tag, Nil))
  }

  private[this] def doDispatch(m: Message): Unit = {
    if (dispatches.containsKey(m.tag)) duplicateTagDetected(m.tag)
    else Local.let(locals) {
      lessor.observeArrival()
      val responseF = Processor(m, service)
      val elapsed = Stopwatch.start()
      val dispatch = Dispatch(m.tag, responseF, elapsed)
      dispatches.put(m.tag, dispatch)

      // Concurrency is controlled by the serial executor so we must
      // bounce the result through it.
      responseF.respond { result =>
        serialExecutor.execute(new Runnable {
          def run(): Unit = renderResponse(dispatch, result)
        })
      }
    }
  }

  private[this] def renderResponse(dispatch: Dispatch, response: Try[Message]): Unit = {
    dispatches.remove(dispatch.tag) match {
      case null => // nop: the dispatch was discarded and the tag is unused

      case storedDispatch if storedDispatch eq dispatch =>
        val result = response match {
          case Return(rep) =>
            lessor.observe(dispatch.timer())
            rep
          case Throw(exc) =>
            log.warning(exc, s"Error processing message from ($remoteAddress)")
            Message.Rerr(dispatch.tag, exc.toString)
        }

        messageWriter.write(result)
        checkDrained()

      case storedDispatch =>
        // This was the result of a race between a request that was canceled
        // and its tag being reused, and the dispatch of the initial request.
        // We discard this result and place the dispatch back in the map so it
        //  can be handled.
        dispatches.put(storedDispatch.tag, storedDispatch)
    }
  }

  // Should be called every time an element is removed from the dispatch map.
  private[this] def checkDrained(): Unit = {
    if (state == Draining && dispatches.isEmpty) {
      // The dispatches are all finished, so lets make sure the write manager is finished
      state = Closed
      messageWriter.drain().respond(drainedP.updateIfEmpty(_))
    }
  }

  private[this] def duplicateTagDetected(tag: Int): Unit = {
    // We have a pathological client which is sending duplicate tags. This represents
    // a protocol fault, and we abort all pending requests and close the session.
    // Note: we don't send two Rerr's for the dispatches that had the duplicate tag
    val msg = s"Received duplicate tag ${tag} from client."
    log.warning(msg)
    statsReceiver.counter("duplicate_tag").incr()

    val pending = takeAllDispatches()
    val exc = new InterruptedException

    pending.foreach { dispatch =>
      messageWriter.write(Rerr(dispatch.tag, msg))
      dispatch.response.raise(exc)
    }

    // We drain the `MessageWriter` and then complete our drain future with an exception
    // so that we let everyone know we had a bad peer.
    state = Closed
    messageWriter.drain.ensure {
      drainedP.updateIfEmpty(Throw(new IllegalStateException(msg)))
    }
  }

  private[this] def takeAllDispatches(): Vector[Dispatch] = {
    // get all the elements, then clear so that the dispatches are definitely cleared
    // before we raise on them, which may cause them to finish with the InterruptedException
    val pending = dispatches.values.asScala.toVector
    dispatches.clear()
    pending
  }
}

private object ServerTracker {

  private val log = Logger.get()

  private case class Dispatch(
    tag: Int,
    response: Future[Message],
    timer: Stopwatch.Elapsed
  )

  // Representation of the draining state of the session
  sealed abstract class DrainState(override val toString: String)
  object Open extends DrainState("Open")
  object Draining extends DrainState("Draining")
  object Closed extends DrainState("Closed")
}
