package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.filter.HttpNackFilter
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.{Dtab, Failure}
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.http.codec.HttpDtab
import com.twitter.finagle.http.{Fields, Status, Request, Response}
import com.twitter.finagle.transport.Transport
import com.twitter.logging.Logger
import com.twitter.util.{Throw, Return, Future, Promise}
import com.twitter.finagle.netty4.http.ReaderUtils.{readChunk, streamChunks}
import io.netty.handler.codec.{http => NettyHttp}

private[http] object HttpClientDispatcher {
  val NackFailure = Throw(Failure.rejected("The request was nacked by the server"))
  private val log = Logger(getClass.getName)
  private val isNack = { res: NettyHttp.HttpResponse =>
    res.status.code == HttpNackFilter.ResponseStatus.code &&
    res.headers.contains(HttpNackFilter.Header)
  }
}

/**
 * A http 1.1 client dispatcher.
 *
 * @param statsReceiver typically scoped to `clientName/dispatcher`
 */
private[http] class HttpClientDispatcher(
    trans: Transport[Any, Any],
    statsReceiver: StatsReceiver)
  extends GenSerialClientDispatcher[Request, Response, Any, Any](trans, statsReceiver) {

  import GenSerialClientDispatcher.wrapWriteException
  import HttpClientDispatcher._

  def this(trans: Transport[Any, Any]) =
    this(trans, NullStatsReceiver)

  def dispatch(req: Request, p: Promise[Response]): Future[Unit] = {

    val dtabHeaders = HttpDtab.strip(req)
    if (dtabHeaders.nonEmpty) {
      // Log an error immediately if we find any Dtab headers already in the request and report them
      val headersString = dtabHeaders.map({case (k, v) => s"[$k: $v]"}).mkString(", ")
      log.error(s"discarding manually set dtab headers in request: $headersString\n" +
        s"set Dtab.local instead to send Dtab information.")
    }
    HttpDtab.write(Dtab.local, req)

    if (!req.isChunked && !req.headerMap.contains(Fields.ContentLength)) {
      val len = req.getContent().readableBytes
      // Only set the content length if we are sure there is content. This
      // behavior complies with the specification that user agents should not
      // set the content length header for messages without a payload body.
      if (len > 0) req.headerMap.set(Fields.ContentLength, len.toString)
    }

    val nettyReq = Bijections.finagle.requestToNetty(req)
    trans.write(nettyReq).rescue(wrapWriteException).before {
      // 1. Drain the Request body into the Transport.
      val reqStreamF =
        if (req.isChunked) streamChunks(trans, req.reader)
        else Future.Done

      // 2. Drain the Transport into Response body.
      val repF = trans.read().flatMap {
        case res: NettyHttp.HttpResponse if isNack(res) =>
          p.updateIfEmpty(NackFailure)
          Future.Done

        case rep: NettyHttp.HttpResponse =>
          // unchunked response
          val finagleRep = Bijections.netty.responseToFinagle(rep)
          p.updateIfEmpty(Return(finagleRep))
          Future.Done

        case rep: NettyHttp.HttpContent =>
          // chunked response
          val coll = Transport.collate(trans, readChunk)
          p.updateIfEmpty(Return(Response(req.version, Status.Ok, coll)))
          coll

        case invalid =>
          // relies on GenSerialClientDispatcher satisfying `p`
          Future.exception(new IllegalArgumentException(s"invalid message '$invalid'"))
      }

      Future.join(reqStreamF, repF).unit

    }.onFailure { _ =>
      req.reader.discard()
      trans.close()
    }
  }
}

