package com.twitter.finagle.http.codec

import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.{Response, Ask, ReaderUtils}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.Dtab
import com.twitter.io.{Reader, BufReader}
import com.twitter.util.{Future, Promise, Return, Throw}
import org.jboss.netty.handler.codec.http.{
  HttpRequest => HttpAsk, HttpResponse, HttpHeaders
}

/**
 * Client dispatcher for HTTP.
 *
 * The dispatcher modifies each request with Dtab encoding and streams chunked
 * responses via `Reader`.
 */
class HttpClientDispatcher[Req <: HttpAsk](
  trans: Transport[Any, Any]
) extends GenSerialClientDispatcher[Req, Response, Any, Any](trans) {

  import GenSerialClientDispatcher.wrapWriteException
  import ReaderUtils.{readChunk, streamChunks}

  // BUG: if there are multiple requests queued, this will close a connection
  // with pending dispatches.  That is the right thing to do, but they should be
  // re-queued. (Currently, wrapped in a WriteException, but in the future we
  // should probably introduce an exception to indicate re-queueing -- such
  // "errors" shouldn't be counted against the retry budget.)
  protected def dispatch(req: Req, p: Promise[Response]): Future[Unit] = {
    // It's kind of nasty to modify the request inline like this, but it's
    // in-line with what we already do in finagle-http. For example:
    // the body buf gets read without slicing.
    HttpDtab.clear(req)
    HttpDtab.write(Dtab.local, req)

    if (!req.isChunked && !HttpHeaders.isContentLengthSet(req)) {
      val len = req.getContent().readableBytes
      // Only set the content length if we are sure there is content. This
      // behavior complies with the specification that user agents should not
      // set the content length header for messages without a payload body.
      if (len > 0) HttpHeaders.setContentLength(req, len)
    }
    
    val reader = req match {
      case r: Ask if r.isChunked => Some(r.reader)
      case _ => None
    }
    
    trans.write(req) rescue(wrapWriteException) before {
      // Do these concurrently:
      Future.join(
        // 1. Drain the Ask body into the Transport.
        // (If we have a reader.)
        reader.map(streamChunks(trans, _)).getOrElse(Future.Done),

        // 2. Drain the Transport into Response body.
        trans.read() flatMap {
          case res: HttpResponse if !res.isChunked =>
            val response = new Response {
              final val httpResponse = res
              override val reader = BufReader(ChannelBufferBuf.Owned(res.getContent))
            }

            p.updateIfEmpty(Return(response))
            Future.Done

          case res: HttpResponse =>
            val coll = Transport.collate(trans, readChunk)

            p.updateIfEmpty(Return(new Response {
              final val httpResponse = res
              override val reader = coll
            }))

            coll

          case invalid =>
            // We rely on the base class to satisfy p.
            Future.exception(new IllegalArgumentException(
                "invalid message \"%s\"".format(invalid)))
        }
      ).unit
    } onFailure { case _ =>
      // This Future represents the totality of the exchange;
      // thus failure represents *any* failure that can happen
      // during the exchange.
      reader.foreach(_.discard())
      trans.close()
    }
  }
}
