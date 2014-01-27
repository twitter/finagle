package com.twitter.finagle.http.codec

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.{HttpTransport, Response}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Dtab
import com.twitter.io.{Buf, Writer}
import com.twitter.util.{Future, Promise, Return}
import org.jboss.netty.handler.codec.http.{HttpChunk, HttpRequest, HttpResponse}

/**
 * Client dispatcher for HTTP.
 *
 * The dispatcher modifies each request with Dtab encoding and streams chunked
 * responses via `Reader`.
 */
class HttpClientDispatcher[Req <: HttpRequest](
  transIn: Transport[Any, Any]
) extends GenSerialClientDispatcher[Req, Response, Any, Any](transIn) {

  import GenSerialClientDispatcher.wrapWriteException

  private[this] val trans = new HttpTransport(transIn)

  private[this] def readChunks(writer: Writer): Future[Unit] =
    trans.read() flatMap {
      case chunk: HttpChunk if chunk.isLast =>
        writer.write(Buf.Eof)

      case chunk: HttpChunk =>
        writer.write(ChannelBufferBuf(chunk.getContent)) before
          readChunks(writer)

      case invalid =>
        trans.close()
        writer.fail(
          new IllegalArgumentException(
            "invalid message \"%s\"".format(invalid)))
        // Because we resolve the promise before this point, a returned
        // exception will be unused: the caller of dispatch responds by calling
        // p.updateIfEmpty(..), which is a no-op since we know p to be resolved.
        // We instead return Future.Done to show that the returned value need
        // not be meaningful.
        Future.Done
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
          val response = Response(res)
          p.updateIfEmpty(Return(response))
          readChunks(response.writer)

        case invalid =>
          Future.exception(
            new IllegalArgumentException(
              "invalid message \"%s\"".format(invalid)))
      }
  }
}
