package com.twitter.finagle.stream

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Return, Throw}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http._

/**
 * Stream chunks into StreamResponses.
 */
private class StreamClientDispatcher(trans: Transport[Any, Any])
    extends GenSerialClientDispatcher[HttpRequest, StreamResponse, Any, Any](trans) {
  import GenSerialClientDispatcher.wrapWriteException

  private[this] def readChunks(out: Broker[ChannelBuffer]): Future[Unit] =
    trans.read() flatMap {
      case chunk: HttpChunk if chunk.isLast =>
        Future.Done

      case chunk: HttpChunk =>
        out.send(chunk.getContent).sync() flatMap { unit => readChunks(out) }

      case invalid =>
        Future.exception(
          new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
    }

  protected def dispatch(req: HttpRequest, p: Promise[StreamResponse]) =
    trans.write(req) rescue(wrapWriteException) flatMap { unit =>
      trans.read() flatMap {
        case httpRes: HttpResponse =>
          val out = new Broker[ChannelBuffer]
          val err = new Broker[Throwable]
          val done = new Promise[Unit]

          if (!httpRes.isChunked) {
            val content = httpRes.getContent
            if (content.readable) out ! content
            done.setDone()
          } else {
            readChunks(out) respond {
              case Return(()) | Throw(_: ChannelClosedException) =>
                err ! EOF
              case Throw(exc) =>
                err ! exc
            } ensure { done.setDone() }
          }

          val res = new StreamResponse {
            val httpResponse = httpRes
            val messages = out.recv
            val error = err.recv
            def release() { done.setDone() }
          }
          p.updateIfEmpty(Return(res))

          done ensure {
            trans.close()
          }

        case invalid =>
          Future.exception(
            new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
      }
    }
}
