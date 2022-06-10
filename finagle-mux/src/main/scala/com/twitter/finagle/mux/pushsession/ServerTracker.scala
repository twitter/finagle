package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Service
import com.twitter.finagle.client.BackupRequestFilter
import com.twitter.finagle.mux.ClientDiscardedRequestException
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.Response
import com.twitter.finagle.mux.ServerProcessor
import com.twitter.finagle.mux.lease.exp.Lessee
import com.twitter.finagle.mux.lease.exp.Lessor
import com.twitter.finagle.mux.lease.exp.nackOnExpiredLease
import com.twitter.finagle.mux.pushsession.ServerTracker._
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message.Rdiscarded
import com.twitter.finagle.mux.transport.Message.Rerr
import com.twitter.finagle.mux.transport.Message.Tdispatch
import com.twitter.finagle.mux.transport.Message.Treq
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
 * Class responsible for tracking outstanding dispatches, including during draining. All public
 * methods of this class are intended to be used from within the serial executor associated
 * with the session.
 */
private class ServerTracker(
  serialExecutor: Executor,
  locals: () => Local.Context,
  service: Service[Request, Response],
  h_messageWriter: MessageWriter,
  lessor: Lessor,
  statsReceiver: StatsReceiver,
  remoteAddress: SocketAddress) {

  // We use the netty type because it results in less allocations, but this could
  // just as well be a java Map if we wanted.
  private[this] val h_dispatches = new IntObjectHashMap[Dispatch]

  private[this] val drainedP = Promise[Unit]()
  // Note, the vars below are only read within the context of
  // the `serialExecutor`, thus, we don't need any memory barriers.
  private[this] var h_state: DrainState = Open
  private[this] var h_leaseExpiration: Time = Time.Top
  private[this] var h_cachedLocals: Option[Local.Context] = None

  /** `Lessee` used for issuing lease duration. */
  val lessee: Lessee = new Lessee {
    def issue(howLong: Duration): Unit = {
      require(howLong >= Message.Tlease.MinLease)

      // The update of the leaseExpiration and the write of that expiration must be done in
      // one atomic step to ensure that we keep a consistent view of the lease between the
      // client and the server.
      serialExecutor.execute(new Runnable {
        def run(): Unit = {
          h_leaseExpiration = Time.now + howLong
          h_messageWriter.write(Message.Tlease(howLong.min(Message.Tlease.MaxLease)))
        }
      })
    }

    // The value produced is subject to races in the underlying hashmap but since
    // the value produced is already racy in nature we accept it.
    def npending(): Int = h_dispatches.size
  }

  /**
   * Interrupt all outstanding dispatches, raising on their `Future` with
   */
  def interruptOutstandingDispatches(exc: Throwable): Unit = {
    // get all the elements, then clear so that the dispatches are definitely cleared
    // before we raise on them.
    val pending = handleTakeAllDispatches()
    log.debug(exc, "Interrupting %d dispatches", pending.size)
    pending.foreach(_.response.raise(exc))
  }

  private[this] def canDispatch: Boolean =
    h_state == Open && !(nackOnExpiredLease() && h_leaseExpiration <= Time.now)

  /**
   * Provide the current state of the tracker
   */
  def currentState: DrainState = h_state

  /**
   * Called when receiving a Tdiscarded from the client
   *
   * This call will handle the details of writing a response to the peer, if necessary.
   */
  def discarded(tag: Int, why: String): Unit = {
    h_dispatches.remove(tag) match {
      case null =>
        if (h_messageWriter.removeForTag(tag) == MessageWriter.DiscardResult.NotFound) {
          // We either never had a dispatch and the client is misbehaving or
          // or we already sent the response so the tag is freed in servers eyes.

          // This stat is intentionally created on-demand as this event is infrequent enough to
          // outweigh the benefit of a persistent counter.
          statsReceiver.counter("orphaned_tdiscard").incr()
        } else {
          // We had something queued for write so send an `Rdiscarded` to let
          // the peer know that we've aborted the dispatch.
          h_messageWriter.write(Rdiscarded(tag))
        }

      case dispatch =>
        // We raise on the dispatch and immediately send back a Rdiscarded
        if (isSupersededBackupRequestException(why)) {
          dispatch.response.raise(newSupersededBackupRequestException(why))
        } else {
          dispatch.response.raise(new ClientDiscardedRequestException(why))
        }

        h_messageWriter.write(Rdiscarded(tag))

        // By removing a message we may have finished draining, so check.
        handleCheckDrained()
    }
  }

  /**
   * Signals the tracker to begin the draining process
   *
   * When all pending dispatches are drained the `MessageWriter` will be drained as well.
   */
  def drain(): Unit = {
    if (h_state == Open) {
      h_state = Draining
      handleCheckDrained()
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
    if (canDispatch) handleDispatch(request)
    else h_messageWriter.write(Message.RreqNack(request.tag))
  }

  /**
   * Dispatch a Tdispatch message
   */
  def dispatch(request: Tdispatch): Unit = {
    if (canDispatch) handleDispatch(request)
    else h_messageWriter.write(Message.RdispatchNack(request.tag, Nil))
  }

  private[this] def handleGetLocals(): Local.Context = {
    h_cachedLocals match {
      case Some(ls) => ls
      case None =>
        val ls = locals()
        h_cachedLocals = Some(ls)
        ls
    }
  }

  private[this] def handleDispatch(m: Message): Unit = {
    if (h_dispatches.containsKey(m.tag)) handleDuplicateTagDetected(m.tag)
    else
      Local.let(handleGetLocals()) {
        lessor.observeArrival()
        val responseF = ServerProcessor(m, service)
        val elapsed = Stopwatch.start()
        val dispatch = Dispatch(m.tag, responseF, elapsed)
        h_dispatches.put(m.tag, dispatch)

        // Concurrency is controlled by the serial executor so we must
        // bounce the result through it.
        responseF.respond { result =>
          serialExecutor.execute(new Runnable {
            def run(): Unit = handleRenderResponse(dispatch, result)
          })
        }
      }
  }

  private[this] def handleRenderResponse(dispatch: Dispatch, response: Try[Message]): Unit = {
    h_dispatches.remove(dispatch.tag) match {
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

        h_messageWriter.write(result)
        handleCheckDrained()

      case storedDispatch =>
        // This was the result of a race between a request that was canceled
        // and its tag being reused, and the dispatch of the initial request.
        // We discard this result and place the dispatch back in the map so it
        //  can be handled.
        h_dispatches.put(storedDispatch.tag, storedDispatch)
    }
  }

  // Should be called every time an element is removed from the dispatch map.
  private[this] def handleCheckDrained(): Unit = {
    if (h_state == Draining && h_dispatches.isEmpty) {
      // The dispatches are all finished, so lets make sure the write manager is finished
      h_state = Closed
      h_messageWriter.drain().respond(drainedP.updateIfEmpty(_))
    }
  }

  private[this] def handleDuplicateTagDetected(tag: Int): Unit = {
    // We have a pathological client which is sending duplicate tags. This represents
    // a protocol fault, and we abort all pending requests and close the session.
    // Note: we don't send two Rerr's for the dispatches that had the duplicate tag
    val msg = s"Received duplicate tag ${tag} from client."
    log.warning(msg)

    // This stat is intentionally created on-demand as this event is infrequent enough to
    // outweigh the benefit of a persistent counter.
    statsReceiver.counter("duplicate_tag").incr()

    val pending = handleTakeAllDispatches()
    val exc = new InterruptedException

    pending.foreach { dispatch =>
      h_messageWriter.write(Rerr(dispatch.tag, msg))
      dispatch.response.raise(exc)
    }

    // We drain the `MessageWriter` and then complete our drain future with an exception
    // so that we let everyone know we had a bad peer.
    h_state = Closed
    h_messageWriter.drain.ensure {
      drainedP.updateIfEmpty(Throw(new IllegalStateException(msg)))
    }
  }

  private[this] def handleTakeAllDispatches(): Vector[Dispatch] = {
    // get all the elements, then clear so that the dispatches are definitely cleared
    // before we raise on them, which may cause them to finish with the InterruptedException
    val pending = h_dispatches.values.asScala.toVector
    h_dispatches.clear()
    pending
  }
}

private object ServerTracker {

  private val log = Logger.get()

  private def newSupersededBackupRequestException(why: String): Exception =
    new ClientDiscardedRequestException(why, FailureFlags.Interrupted | FailureFlags.Ignorable)

  // cases included:
  // 1. the SupersededRequestFailure thrown by BackRequestFilter,
  //    e.g.: Failure(Request was superseded by another in BackupRequestFilter, ...)
  // 2. Multi-level propagation that the SupersededRequestFailure is wrapped by ClientDiscardedRequestException
  // 3. Other wrappers (in-between hop has not been updated to this version) over SupersededRequestFailure
  private def isSupersededBackupRequestException(failureMessage: String): Boolean =
    failureMessage.contains(BackupRequestFilter.SupersededRequestFailureWhy)

  private case class Dispatch(tag: Int, response: Future[Message], timer: Stopwatch.Elapsed)

  // Representation of the draining state of the session
  sealed abstract class DrainState(override val toString: String)
  object Open extends DrainState("Open")
  object Draining extends DrainState("Draining")
  object Closed extends DrainState("Closed")
}
