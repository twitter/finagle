package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message._
import com.twitter.finagle.pushsession.PushChannelHandle
import com.twitter.finagle.mux.pushsession.MessageWriter.DiscardResult
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.io.Buf
import com.twitter.logging.{HasLogLevel, Level, Logger}
import com.twitter.util.{Future, Promise, Return, Throw, Try}
import java.util
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

/**
 * Abstraction for queuing mux messages for serialization to the socket
 *
 * @note Implementations are expected to be _very_ stateful and expects
 *       to enjoy the benefits of executing inside of a `PushSession`s
 *       serial `Executor`.
 */
private[finagle] trait MessageWriter {

  /**
   * Write a message to the wire
   */
  def write(message: Message): Unit

  /**
   * Remove any pending writes for the specified tag
   */
  def removeForTag(id: Int): DiscardResult

  /**
   * Start draining the writer, providing a Future which is satisfied once
   * draining has completed.
   */
  def drain(): Future[Unit]
}

private[finagle] object MessageWriter {

  /** Result of attempting to remove a tag from the `MessageWriter` */
  sealed trait DiscardResult
  object DiscardResult {

    /**
     * Indicates that message was not found for that tag. This happens if there was
     * no write or if the write completed.
     */
    case object NotFound extends DiscardResult

    /**
     * A whole message was removed meaning no fragments have been sent to the peer.
     */
    case object Unwritten extends DiscardResult

    /**
     * Removed the tail of a fragment stream meaning at least one chunk has been
     * sent to the peer.
     */
    case object PartialWrite extends DiscardResult
  }
}

/**
 * Write mux `Message`s efficiently, fragmenting `Fragmentable` messages
 * into chunks of size `windowBytes` or less.
 *
 * @note this class is _very_ stateful and expects to enjoy the benefits of executing inside of
 *       a `PushSession`s serial `Executor`.
 */
private[finagle] final class FragmentingMessageWriter(
  handle: PushChannelHandle[_, Buf],
  windowBytes: Int,
  stats: SharedNegotiationStats)
    extends MessageWriter {

  // While this constructor defeats the purprose of sharing stats between multiple writers,
  // it's convenient in testing.
  def this(handle: PushChannelHandle[_, Buf], windowBytes: Int, stats: StatsReceiver) =
    this(handle, windowBytes, new SharedNegotiationStats(stats))

  import FragmentingMessageWriter._

  // The messages must have normalized tags, even fragments
  private[this] val messageQueue = new util.ArrayDeque[Message]

  // State of the MessageWriter. Transitions can be:
  // Idle <-> Flushing
  // Flushing -> Draining
  // Idle, Flushing, Draining -> Closed
  private[this] var state: State = Idle

  def drain(): Future[Unit] = state match {
    case Idle =>
      state = Closed(Return.Unit)
      Future.Done

    case Flushing =>
      val p = Promise[Unit]()
      state = Draining(p)
      p

    case Draining(p) =>
      p

    case Closed(cause) =>
      Future.const(cause)
  }

  def write(msg: Message): Unit = {
    if (msg.isInstanceOf[Fragment])
      throw new IllegalStateException(s"Only full messages are valid: $msg")

    state match {
      case Idle =>
        messageQueue.add(msg)
        stats.pendingWriteStreams.increment()
        state = Flushing
        batchWrite()

      case _: FlushingState =>
        // Add message to the queue for the pending write to handle
        messageQueue.add(msg)
        stats.pendingWriteStreams.increment()

      case Closed(Throw(ex)) =>
        log.debug(ex, "Discarding message %s due to previous write failure", msg)

      case Closed(Return(_)) =>
        log.debug("Discarding message %s due to being closed", msg)
    }
  }

  def removeForTag(tag: Int): DiscardResult = {
    var result: DiscardResult = DiscardResult.NotFound

    // We need to go through the whole queue even after we've found the element
    // we want so that we don't re-prioritize a tags before the removed stream
    // with respect to the tags after the removed fragment.
    @tailrec
    def cleanLoop(numFragments: Int): Unit = {
      if (numFragments > 0) {
        val msg = messageQueue.poll
        // Add all the elements back that don't match the specified tag
        if (msg.tag != tag) {
          messageQueue.add(msg)
        } else if (result != DiscardResult.NotFound) {
          val ex = new IllegalStateException(s"Found multiple fragments for tag $tag")
          onError(ex)
        } else {
          result = msg match {
            case Fragment(_, _, _) => DiscardResult.PartialWrite
            case _ => DiscardResult.Unwritten
          }
        }

        cleanLoop(numFragments - 1)
      }
    }

    val sizeBeforeCleaning = messageQueue.size
    cleanLoop(sizeBeforeCleaning)
    stats.pendingWriteStreams.add(messageQueue.size() - sizeBeforeCleaning)

    result
  }

  private[this] val lastWrite: Try[Unit] => Unit = {
    case Return(_) =>
      state match {
        case Idle => // should never happen
          throw new IllegalStateException("After completing write, found state to be Idle")

        case Flushing =>
          if (!messageQueue.isEmpty) batchWrite()
          else {
            state = Idle
          }

        case Draining(p) =>
          if (!messageQueue.isEmpty) batchWrite()
          else {
            // finished
            state = Closed(Return.Unit)
            p.setDone()
          }

        case Closed(Throw(ex)) =>
          log.debug(ex, "Write completed to find closed-with-error state")

        case Closed(Return(_)) =>
          log.debug("Write completed to find closed state")
      }

    case Throw(ex) =>
      onError(ex)
  }

  private[this] def onError(ex: Throwable): Unit = state match {
    case Closed(_) => // nop, already closed
    case _ =>
      state = Closed(Throw(ex))
      handle.close()

      val logLevel = ex match {
        case hasLevel: HasLogLevel => hasLevel.logLevel
        case _ => Level.WARNING
      }

      log.log(logLevel, ex, "session closed due to exception")
  }

  // Schedule writes to the socket, notifying `lastWrite` when its done.
  private[this] def batchWrite(): Unit = {
    assert(state.isInstanceOf[FlushingState])
    assert(!messageQueue.isEmpty)

    var i = messageQueue.size

    if (i == 1) {
      // fast path: no need to allocate a collection. This is a micro-opt.
      val msgBuf = takeBufFragment()
      handle.send(msgBuf)(lastWrite)
    } else {
      // Multiple messages. Prepare and load them into an ArrayBuffer.
      // Note: we don't accumulate the Buf because the pipeline will
      // be responsible for prepending the lengths.
      val messages = new ArrayBuffer[Buf](i)
      while (i > 0) {
        i -= 1
        messages += takeBufFragment()
      }

      handle.send(messages)(lastWrite)
    }
  }

  private[this] def fitsInSingleFragment(msg: Message): Boolean =
    msg.buf.length <= windowBytes

  private[this] def needsFragmenting(msg: Message): Boolean = msg match {
    case _: Fragment | _: Fragmentable => !fitsInSingleFragment(msg)
    case _ => false
  }

  // Take a message chunk and encode it for writing to the wire.
  // Note: Presumes at least one chunk exists in the write queue
  private[this] def takeBufFragment(): Buf = {
    val msg = takeFragment()
    val msgBuf = Message.encode(msg)
    stats.writeStreamBytes.add(msgBuf.length)
    msgBuf
  }

  // Take a chunk of a pending message, restoring any leftovers at the end of the write queue.
  // Note: Presumes at least one chunk exists in the write queue
  private[this] def takeFragment(): Message = {
    val msg = messageQueue.poll()
    if (!needsFragmenting(msg)) {
      stats.pendingWriteStreams.decrement()
      msg
    } else {
      val msgBuf = msg.buf
      assert(msgBuf.length > windowBytes)
      val f1 = Fragment(msg.typ, Tags.setMsb(msg.tag), msgBuf.slice(0, windowBytes))
      val remainder = Fragment(msg.typ, msg.tag, msgBuf.slice(windowBytes, Int.MaxValue))
      messageQueue.add(remainder)
      f1
    }
  }
}

private object FragmentingMessageWriter {
  private val log = Logger.get

  private sealed trait State

  private case object Idle extends State

  private sealed trait FlushingState extends State
  private case object Flushing extends FlushingState
  private case class Draining(listeners: Promise[Unit]) extends FlushingState

  private case class Closed(cause: Try[Unit]) extends State
}
