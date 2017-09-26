package com.twitter.finagle.mux.exp.pushsession

import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.transport.Message._
import com.twitter.finagle.exp.pushsession.PushChannelHandle
import com.twitter.finagle.mux.exp.pushsession.MessageWriter.DiscardResult
import com.twitter.io.Buf
import com.twitter.logging.Logger
import com.twitter.util.{Return, Throw, Try}
import java.util
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

private[finagle] trait MessageWriter {

  /**
   * Write a message to the wire
   */
  def write(message: Message): Unit

  /**
   * Remove any pending writes for the specified tag
   */
  def removeForTag(id: Int): DiscardResult
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
private final class FragmentingMessageWriter(handle: PushChannelHandle[_, Buf], windowBytes: Int)
    extends MessageWriter {

  // The messages must have normalized tags, even fragments
  // Exposed for testing
  private[pushsession] val messageQueue = new util.ArrayDeque[Message]
  private[this] val log = Logger.get
  // State so we don't add log for every outstanding write
  private[this] var observedWriteError: Boolean = false

  // We use this boolean to ensure that we have at most one flush loop at any time. If
  // we allow n loops we can end up effectively flushing n fragments for every time the
  // byte stream is flushed which defeats the purpose of the fragmenting.
  private[this] var flushing = false

  private[this] def onWriteError(ex: Throwable): Unit = {
    if (!observedWriteError) {
      observedWriteError = true
      log.info(s"Error obtained during write phase: $ex", ex)
      handle.close()
    }
  }

  def write(msg: Message): Unit = msg match {
    case f: Fragment => // should never happen
      throw new IllegalStateException(s"Only full messages are valid: $f")

    case _ =>
      messageQueue.add(msg)
      checkWrite()
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
          onWriteError(ex)
        } else {
          result = msg match {
            case Fragment(_, _, _) => DiscardResult.PartialWrite
            case _ => DiscardResult.Unwritten
          }
        }

        cleanLoop(numFragments - 1)
      }
    }

    cleanLoop(messageQueue.size)

    result
  }

  private[this] val lastWrite: Try[Unit] => Unit = {
    case Return(_) =>
      flushing = false
      checkWrite() // we might start right back up again

    case Throw(ex) =>
      onWriteError(ex)
  }

  // Checks if we're waiting on a flush, and if not, schedule writes to the socket.
  private[this] def checkWrite(): Unit = {
    if (!flushing && !messageQueue.isEmpty) {
      flushing = true
      var i = messageQueue.size

      if (i == 1) {
        // fast path: no need to allocate a collection. This is a micro-opt.
        val rawMsg = messageQueue.poll()
        val msg = takeFragment(rawMsg)
        handle.send(Message.encode(msg))(lastWrite)
      } else {
        // Multiple messages. Prepare and load them into an ArrayBuffer.
        // Note: we don't accumulate the Buf because the pipeline will
        // be responsible for prepending the lengths.
        val messages = new ArrayBuffer[Buf](i)
        while (i > 0) {
          i -= 1
          val rawMsg = messageQueue.poll()
          val chunk = takeFragment(rawMsg)
          messages += Message.encode(chunk)
        }

        handle.send(messages)(lastWrite)
      }
    }
  }

  private[this] def fitsInSingleFragment(msg: Message): Boolean =
    msg.buf.length <= windowBytes

  private[this] def needsFragmenting(msg: Message): Boolean = msg match {
    case _: Fragment | _: Fragmentable => !fitsInSingleFragment(msg)
    case _ => false
  }

  // potentially fragments a message, storing any leftovers and returning the chunk to write
  private[this] def takeFragment(msg: Message): Message = {
    if (!needsFragmenting(msg)) msg
    else {
      val msgBuf = msg.buf
      assert(msgBuf.length > windowBytes)
      val f1 = Fragment(msg.typ, Tags.setMsb(msg.tag), msgBuf.slice(0, windowBytes))
      val remainder = Fragment(msg.typ, msg.tag, msgBuf.slice(windowBytes, Int.MaxValue))
      messageQueue.add(remainder)
      f1
    }
  }
}
