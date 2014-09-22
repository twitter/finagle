package com.twitter.finagle.httpx.codec

import com.twitter.concurrent.AsyncMutex
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.httpx.{Response, Request, ReaderUtils}
import com.twitter.finagle.httpx.netty.Bijections._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Dtab
import com.twitter.io.{Buf, Reader, BufReader}
import com.twitter.util.{Future, Promise, Return, Throw}
import org.jboss.netty.handler.codec.http.{
  HttpChunk, HttpRequest, HttpResponse, HttpHeaders
}

/**
 * Client dispatcher for HTTP.
 *
 * The dispatcher modifies each request with Dtab encoding and streams chunked
 * responses via `Reader`.
 */
class HttpClientDispatcher[Req <: Request](
  trans: Transport[Any, Any]
) extends GenSerialClientDispatcher[Req, Response, Any, Any](trans) {

  import GenSerialClientDispatcher.wrapWriteException
  import ReaderUtils.{readerFromTransport, streamChunks}

  // BUG: if there are multiple requests queued, this will close a connection
  // with pending dispatches.  That is the right thing to do, but they should be
  // re-queued. (Currently, wrapped in a WriteException, but in the future we
  // should probably introduce an exception to indicate re-queueing -- such
  // "errors" shouldn't be counted against the retry budget.)
  protected def dispatch(req: Req, p: Promise[Response]): Future[Unit] = {
    // It's kind of nasty to modify the request inline like this, but it's
    // in-line with what we already do in finagle.httpx. For example:
    // the body buf gets read without slicing.
    HttpDtab.write(Dtab.local, req)
    trans.write(from[Request, HttpRequest](req)) rescue(wrapWriteException) before {
      // Do these concurrently:
      Future.join(
        // 1. Drain the Request body into the Transport.
        if (req.isChunked) streamChunks(trans, req.reader) else Future.Done,
        // 2. Drain the Transport into Response body.
        trans.read() flatMap {
          case res: HttpResponse if !res.isChunked =>
            val response = new Response {
              final val httpResponse = res
              override val reader = BufReader(ChannelBufferBuf(res.getContent))
            }

            p.updateIfEmpty(Return(response))
            Future.Done

          case res: HttpResponse =>
            val done = new Promise[Unit]
            val response = new Response {
              final val httpResponse = res
              override val reader = readerFromTransport(trans, done)
            }

            p.updateIfEmpty(Return(response))
            done

          case invalid =>
            // We rely on the base class to satisfy p.
            Future.exception(new IllegalArgumentException(
                "invalid message \"%s\"".format(invalid)))
        }
      ).unit
    }
  }
}
