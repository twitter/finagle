package com.twitter.finagle.http.codec

import com.twitter.concurrent.AsyncMutex
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.Response
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Dtab
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, Promise, Return, Throw}
import org.jboss.netty.handler.codec.http.{HttpChunk, HttpRequest, HttpResponse}

/**
 * Client dispatcher for HTTP.
 *
 * The dispatcher modifies each request with Dtab encoding and streams chunked
 * responses via `Reader`.
 */
class HttpClientDispatcher[Req <: HttpRequest](
  trans: Transport[Any, Any]
) extends GenSerialClientDispatcher[Req, Response, Any, Any](trans) {

  import GenSerialClientDispatcher.wrapWriteException

  private[this] def chunkReader(done: Promise[Unit]) = new Reader { self =>
    private[this] val mu = new AsyncMutex
    @volatile private[this] var buf = Buf.Empty

    // We don't want to rely on scheduler semantics for ordering.
    def read(n: Int): Future[Buf] = mu.acquire() flatMap { permit =>
      val readOp = if (buf eq Buf.Eof) {
        Future.value(Buf.Eof)
      } else if (buf.length > 0) {
        val f = Future.value(buf.slice(0, n))
        buf = buf.slice(n, Int.MaxValue)
        f
      } else trans.read() flatMap {
        // Buf is empty so set it to the result of trans.read()
        case chunk: HttpChunk if chunk.isLast =>
          done.setDone()
          buf = Buf.Eof
          Future.value(Buf.Eof)

        case chunk: HttpChunk =>
          // Read data -- return up to n bytes and save the rest
          val cbb = ChannelBufferBuf(chunk.getContent)
          val f = Future.value(cbb.slice(0, n))
          buf = cbb.slice(n, Int.MaxValue)
          f

        case invalid =>
          val exc = new IllegalArgumentException(
            "invalid message \"%s\"".format(invalid))
          Future.exception(exc)
      }

      readOp onFailure { exc =>
        trans.close()
        done.updateIfEmpty(Throw(exc))
      } ensure { permit.release() }
    }

    def discard() {
      // Any interrupt to `read` will result in transport closure, but we also
      // call `trans.close` here to handle the case where a discard is called
      // without interrupting the `read` operation.
      trans.close()
    }
  }

  // BUG: if there are multiple requests queued, this will close a connection
  // with pending dispatches.  That is the right thing to do, but they should be
  // re-queued. (Currently, wrapped in a WriteException, but in the future we
  // should probably introduce an exception to indicate re-queueing -- such
  // "errors" shouldn't be counted against the retry budget.)
  protected def dispatch(req: Req, p: Promise[Response]): Future[Unit] = {
    // It's kind of nasty to modify the request inline like this, but it's
    // in-line with what we already do in finagle-http. For example:
    // the body buf gets read without slicing.
    HttpDtab.write(Dtab.baseDiff(), req)
    trans.write(req) rescue(wrapWriteException) before
      trans.read() flatMap {
        case res: HttpResponse if !res.isChunked =>
          p.updateIfEmpty(Return(Response(res)))
          Future.Done

        case res: HttpResponse =>
          val done = new Promise[Unit]
          val response = new Response {
            final val httpResponse = res
            override val reader = chunkReader(done)
          }

          p.updateIfEmpty(Return(response))
          done

        case invalid =>
          // We rely on the base class to satisfy p.
          Future.exception(new IllegalArgumentException(
              "invalid message \"%s\"".format(invalid)))
      }
  }
}
