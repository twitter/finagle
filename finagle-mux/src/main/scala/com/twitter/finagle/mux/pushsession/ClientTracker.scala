package com.twitter.finagle.mux.pushsession

import com.twitter.finagle.{ChannelClosedException, Dtab, Failure}
import com.twitter.finagle.mux.{ReqRepHeaders, Request, Response}
import com.twitter.finagle.mux.transport.Message
import com.twitter.finagle.mux.util.TagMap
import com.twitter.finagle.tracing.Trace
import com.twitter.util.{Local, Promise, Throw, Try, Updatable}
import java.net.SocketAddress

/**
 * Data structure and behavior that represents the data plane of a [[MuxClientSession]]
 *
 * @note This structure is not thread safe and expects to be used exclusively within
 *       the serial executor belonging to its parent `MuxClientSession`.
 *
 * @param messageWriter message writer belonging to parent `MuxClientSession`
 */
private class ClientTracker(messageWriter: MessageWriter) {
  import ClientTracker._

  // outstanding messages, each tagged with a unique int between `MinTag` and `MaxTag`.
  private[this] val messages =
    TagMap[Updatable[Try[Response]]](TagRange, InitialTagMapSize)

  def pendingDispatches: Int = messages.size

  def shutdown(oexc: Option[Throwable], remoteAddress: Option[SocketAddress]): Unit = {
    // TODO: Our MessageWriter potentially contains unwritten dispatches which
    // we could mark as retryable since they may have never reached the peer.
    // To do that we need to get the tag and promise from messages so we can
    // dig through the writer to check if they were written or not.
    val remaining = messages.unmapAll()
    val exc = oexc.getOrElse(new ChannelClosedException(None, remoteAddress))
    val reason = Throw(exc)
    remaining.foreach(_.update(reason))
  }

  /** Complete a dispatch response */
  def receivedResponse(tag: Int, msg: Try[Response]): Unit = {
    messages.unmap(tag) match {
      case Some(u) => u() = msg
      case None => ()
    }
  }

  /**
   * Acquire a tag, if possible, and perform a dispatch. If we are able to
   * acquire a tag, it is returned, otherwise -1 is returned.
   *
   * @note this method takes ownership of `dispatchP`.
   */
  def dispatchRequest(
    req: Request,
    asDispatch: Boolean,
    locals: Local.Context,
    dispatchP: Promise[Response]
  ): Int = {
    messages.map(dispatchP) match {
      case None =>
        dispatchP.setException(ExhaustedTagsException)
        // Doesn't matter what we return: the promise is already satisfied so
        // the interrupt handler will never fire.
        -1

      case Some(tag) =>
        val msg = requestToMessage(req, tag, asDispatch, locals)
        messageWriter.write(msg)
        tag
    }
  }

  /** Cleanup session concerns related to a dispatch being interrupted */
  def requestInterrupted(p: Promise[Response], tag: Int, cause: Throwable): Unit = {
    // We replace the current Updatable, if any, with a stand-in to reserve
    // the tag of a discarded request until a Rmessage is sent by the peer.

    messages.maybeRemap(tag, Updatable.Empty) match {
      case Some(u) if u ne p =>
        // We had a tag collision: this interrupt isn't for the promise that was mapped to the
        // tag, so we have to put `u` back and the response must have received a reply
        messages.maybeRemap(tag, u)

      case Some(u) =>
        // We interrupted our dispatch. Need to see if we actually wrote the dispatch
        // and if so, send a Tdiscarded and await a response from the server
        if (messageWriter.removeForTag(tag) == MessageWriter.DiscardResult.Unwritten) {
          messages.unmap(tag)
        } else {
          // Unfortunately, at least part of the message made it to the socket
          // so we need to leave the `Empty` in there and send a `Tdiscarded`.
          messageWriter.write(Message.Tdiscarded(tag, cause.toString))
        }

        u() = Throw(cause)

      case None => // message must have already received a reply
    }
  }

  // Convert a `Request` to a mux `Message`
  private[this] def requestToMessage(
    req: Request,
    tag: Int,
    asDispatch: Boolean,
    locals: Local.Context // Carries the Trace, local Dtabs, and broadcast contexts
  ): Message = {
    val saved = Local.save()
    try {
      Local.restore(locals)
      if (asDispatch) {
        val contexts = ReqRepHeaders.toDispatchContexts(req)
        Message.Tdispatch(tag, contexts.toSeq, req.destination, Dtab.local, req.body)
      } else {
        Message.Treq(tag, Trace.idOption, req.body)
      }
    } finally Local.restore(saved)
  }
}

private object ClientTracker {
  private val TagRange: Range = Message.Tags.MinTag to Message.Tags.MaxTag

  private val InitialTagMapSize: Int = 256

  private val ExhaustedTagsException = Failure.rejected("Exhausted tags")
}
