package com.twitter.finagle.http.codec

import com.twitter.finagle.{Dtab, Failure}
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.{Fields, ReaderUtils, Request, Response}
import com.twitter.finagle.http.exp.{StreamTransport, Multi}
import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.transport.Transport
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
 *
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
private[finagle] class HttpClientDispatcher(
    trans: StreamTransport[Request, Response],
    statsReceiver: StatsReceiver)
  extends GenSerialClientDispatcher[Request, Response, Request, Multi[Response]](
    trans,
    statsReceiver) {

  import HttpClientDispatcher._

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

    // wait on these concurrently:
    Future.join(Seq(
      trans.write(req),
      // Drain the Transport into Response body.
      trans.read().flatMap {
        case Multi(res, _) if HttpNackFilter.isNack(res)=>
          p.updateIfEmpty(Throw(NackFailure))
          Future.Done

        case Multi(res, readFinished) =>
          p.updateIfEmpty(Return(res))
          readFinished
      } // we don't need to satisfy p when we fail because GenSerialClientDispatcher does already
    )).onFailure { _ =>
      // This Future represents the totality of the exchange;
      // thus failure represents *any* failure that can happen
      // during the exchange.
      req.reader.discard()
      trans.close()
    }
  }
}
