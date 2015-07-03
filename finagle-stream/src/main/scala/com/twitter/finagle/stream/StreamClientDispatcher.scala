package com.twitter.finagle.stream

import com.twitter.concurrent.Broker
import com.twitter.finagle.ChannelClosedException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.{Future, Promise, Return, Throw}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpChunk, HttpResponse}

/**
 * Stream chunks into StreamResponses.
 */
private[twitter]
class StreamClientDispatcher[Req: RequestType](trans: Transport[Any, Any])
  extends GenSerialClientDispatcher[Req, StreamResponse, Any, Any](trans) {
  import Bijections._
  import GenSerialClientDispatcher.wrapWriteException

  private[this] val RT = implicitly[RequestType[Req]]

  private[this] def readChunks(out: Broker[Buf]): Future[Unit] =
    trans.read() flatMap {
      case chunk: HttpChunk if chunk.isLast =>
        Future.Done

      case chunk: HttpChunk =>
        out.send(ChannelBufferBuf.Owned(chunk.getContent)).sync() before
          readChunks(out)

      case invalid =>
        Future.exception(
          new IllegalArgumentException("invalid message \"%s\"".format(invalid)))
    }

  protected def dispatch(req: Req, p: Promise[StreamResponse]) =
    trans.write(from(RT.canonize(req)): HttpRequest).rescue(wrapWriteException).before {
      trans.read() flatMap {
        case httpRes: HttpResponse =>
          val out = new Broker[Buf]
          val err = new Broker[Throwable]
          val done = new Promise[Unit]

          if (!httpRes.isChunked) {
            val content = httpRes.getContent
            if (content.readable) out ! ChannelBufferBuf.Owned(content)
            done.setDone()
          } else {
            readChunks(out) respond {
              case Return(_) | Throw(_: ChannelClosedException) =>
                err ! EOF
              case Throw(exc) =>
                err ! exc
            } ensure done.setDone()
          }

          val res = new StreamResponse {
            val info = from(httpRes)
            def messages = out.recv
            def error = err.recv
            def release() = done.setDone()
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
