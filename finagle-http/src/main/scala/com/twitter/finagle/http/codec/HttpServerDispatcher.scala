package com.twitter.finagle.http.codec

import com.twitter.finagle.Service
import com.twitter.finagle.http._
import com.twitter.finagle.http.exp.{GenSerialServerDispatcher, StreamTransport}
import com.twitter.finagle.stats.{StatsReceiver, RollupStatsReceiver}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Throwables}

private[http] object HttpServerDispatcher {
  val handleHttp10: PartialFunction[Throwable, Response] = {
    case _ => Response(Version.Http10, Status.InternalServerError)
  }

  val handleHttp11: PartialFunction[Throwable, Response] = {
    case _ => Response(Version.Http11, Status.InternalServerError)
  }
}

private[finagle] class HttpServerDispatcher(
    trans: StreamTransport[Response, Request],
    service: Service[Request, Response],
    stats: StatsReceiver)
  extends GenSerialServerDispatcher[Request, Response, Response, Request](trans) {
  import HttpServerDispatcher._

  private[this] val failureReceiver =
    new RollupStatsReceiver(stats.scope("stream")).scope("failures")

  trans.onClose.ensure {
    service.close()
  }

  protected def dispatch(m: Request): Future[Response] = m match {
    case badReq: BadReq =>
      val resp = badReq match {
        case _: ContentTooLong =>
          Response(badReq.version, Status.RequestEntityTooLarge)
        case _: UriTooLong =>
          Response(badReq.version, Status.RequestURITooLong)
        case _: HeaderFieldsTooLarge =>
          Response(badReq.version, Status.RequestHeaderFieldsTooLarge)
        case _ =>
          Response(badReq.version, Status.BadRequest)
      }

      // A bad request will most likely result in a corrupt connection, so signal
      // close with a 'Connection: close' header. Closing the transport will then
      // be performed by the ConnectionManager
      resp.headerMap.set(Fields.Connection, "close")
      Future.value(resp)

    case req: Request =>
      val handleFn = req.version match {
        case Version.Http10 => handleHttp10
        case _ => handleHttp11
      }
      service(req).handle(handleFn)

    case invalid =>
      Future.exception(new IllegalArgumentException("Invalid message "+invalid))
  }

  protected def handle(rep: Response): Future[Unit] = {
    setKeepAlive(rep, !isClosing)
    if (rep.isChunked) {
      // We remove content length here in case the content is later
      // compressed. This is a pretty bad violation of modularity;
      // this is likely an issue with the Netty content
      // compressors, which (should?) adjust headers regardless of
      // transfer encoding.
      rep.headerMap.remove(Fields.ContentLength)
      rep.headerMap.set(Fields.TransferEncoding, "chunked")

      val p = new Promise[Unit]
      val f = trans.write(rep)
      f.proxyTo(p)
      // This awkwardness is unfortunate but necessary for now as you may be
      // interrupted in the middle of a write, or when there otherwise isn’t
      // an outstanding read (e.g. read-write race).
      f.onFailure { t =>
        Logger.get(this.getClass.getName).debug(t, "Failed mid-stream. Terminating stream, closing connection")
        failureReceiver.counter(Throwables.mkString(t): _*).incr()
        rep.reader.discard()
      }
      p.setInterruptHandler { case intr =>
        rep.reader.discard()
        f.raise(intr)
      }
      p
    } else {
      // Ensure Content-Length is set if not chunked
      if (!rep.contentLength.isDefined)
        rep.contentLength = rep.content.length

      trans.write(rep)
    }
  }

  /**
   * Set the Connection header as appropriate. This will NOT clobber a 'Connection: close' header,
   * allowing services to gracefully close the connection through the Connection header mechanism.
   */
  private def setKeepAlive(rep: Response, keepAlive: Boolean): Unit = {
    val connectionHeader = rep.headerMap.get(Fields.Connection)
    if (connectionHeader.isEmpty || !"close".equalsIgnoreCase(connectionHeader.get)) {
      rep.version match {
        case Version.Http10 =>
          if (keepAlive) {
            rep.headerMap.set(Fields.Connection, "keep-alive")
          } else {
            rep.headerMap.remove(Fields.Connection)
          }
        case Version.Http11 =>
          if (keepAlive) {
            rep.headerMap.remove(Fields.Connection)
          } else {
            rep.headerMap.set(Fields.Connection, "close")
          }
      }
    }
  }
}
