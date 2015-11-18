package com.twitter.finagle.http.codec

import com.twitter.finagle.{Dtab, Failure}
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.{Fields, ReaderUtils, Request, Response}
import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.http.netty.Bijections._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.io.BufReader
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Return, Throw}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

private[http] object HttpClientDispatcher {
  val NackFailure = Failure.rejected("The request was nacked by the server")
  private val log = Logger(getClass.getName)
}

/**
 * Client dispatcher for HTTP.
 *
 * The dispatcher modifies each request with Dtab encoding from Dtab.local
 * and streams chunked responses via `Reader`.  If the request already contains
 * Dtab headers they will be stripped and an error will be logged.
 */
class HttpClientDispatcher(trans: Transport[Any, Any])
  extends GenSerialClientDispatcher[Request, Response, Any, Any](trans) {

  import GenSerialClientDispatcher.wrapWriteException
  import HttpClientDispatcher._
  import ReaderUtils.{readChunk, streamChunks}

  // BUG: if there are multiple requests queued, this will close a connection
  // with pending dispatches.  That is the right thing to do, but they should be
  // re-queued. (Currently, wrapped in a WriteException, but in the future we
  // should probably introduce an exception to indicate re-queueing -- such
  // "errors" shouldn't be counted against the retry budget.)
  protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] = {
    val dtabHeaders = HttpDtab.strip(req)
    if (dtabHeaders.nonEmpty) {
      // Log an error immediately if we find any Dtab headers already in the request and report them
      val headersString = dtabHeaders.map({case (k, v) => s"[$k: $v]"}).mkString(", ")
      log.error(s"discarding manually set dtab headers in request: $headersString\n" +
        s"set Dtab.local instead to send Dtab information.")
    }

    // It's kind of nasty to modify the request inline like this, but it's
    // in-line with what we already do in finagle-http. For example:
    // the body buf gets read without slicing.
    HttpDtab.write(Dtab.local, req)

    if (!req.isChunked && !req.headerMap.contains(Fields.ContentLength)) {
      val len = req.getContent().readableBytes
      // Only set the content length if we are sure there is content. This
      // behavior complies with the specification that user agents should not
      // set the content length header for messages without a payload body.
      if (len > 0) req.headerMap.set(Fields.ContentLength, len.toString)
    }

    trans.write(from[Request, HttpRequest](req)) rescue(wrapWriteException) before {
      // Do these concurrently:
      Future.join(
        // 1. Drain the Request body into the Transport.
        if (req.isChunked) streamChunks(trans, req.reader)
        else Future.Done,
        // 2. Drain the Transport into Response body.
        trans.read() flatMap {
          case res: HttpResponse if HttpNackFilter.isNack(res) =>
            p.updateIfEmpty(Throw(NackFailure))
            Future.Done

          case res: HttpResponse if !res.isChunked =>
            val response = Response(res, BufReader(ChannelBufferBuf.Owned(res.getContent)))
            p.updateIfEmpty(Return(response))
            Future.Done

          case res: HttpResponse =>
            val coll = Transport.collate(trans, readChunk)
            p.updateIfEmpty(Return(Response(res, coll)))
            coll

          case invalid =>
            // We rely on the base class to satisfy p.
            Future.exception(new IllegalArgumentException(s"invalid message '$invalid'"))
        }
      ).unit
    } onFailure { _ =>
      // This Future represents the totality of the exchange;
      // thus failure represents *any* failure that can happen
      // during the exchange.
      req.reader.discard()
      trans.close()
    }
  }
}
