package com.twitter.finagle.http.codec

import com.twitter.finagle.Service
import com.twitter.finagle.http._
import com.twitter.finagle.stats.CategorizingExceptionStatsHandler
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.logging.Logger
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.finagle.http.GenStreamingSerialServerDispatcher

private[http] object HttpServerDispatcher {
  val handleHttp10: PartialFunction[Throwable, Response] = {
    case _ => Response(Version.Http10, Status.InternalServerError)
  }

  val handleHttp11: PartialFunction[Throwable, Response] = {
    case _ => Response(Version.Http11, Status.InternalServerError)
  }

  private val logger = Logger.get(getClass())
  private val exceptionStatsHandler = new CategorizingExceptionStatsHandler()
}

private[finagle] class HttpServerDispatcher(
  trans: StreamTransport[Response, Request],
  underlying: Service[Request, Response],
  statsReceiver: StatsReceiver)
    extends GenStreamingSerialServerDispatcher[Request, Response, Response, Request](trans) {
  import HttpServerDispatcher._

  // Response conformance (length headers, etc) is performed by the `ResponseConformanceFilter`
  private[this] val service = ResponseConformanceFilter.andThen(underlying)

  trans.onClose.ensure {
    service.close()
  }

  protected def dispatch(req: Request): Future[Response] = {
    val handleFn = req.version match {
      case Version.Http10 => handleHttp10
      case _ => handleHttp11
    }
    service(req).handle(handleFn)
  }

  protected def handle(rep: Response): Future[Unit] = {
    setKeepAlive(rep, !isClosing)

    if (rep.isChunked) {
      val p = new Promise[Unit]
      val f = trans.write(rep)
      f.proxyTo(p)
      // This awkwardness is unfortunate but necessary for now as you may be
      // interrupted in the middle of a write, or when there otherwise isn’t
      // an outstanding read (e.g. read-write race).
      f.onFailure { t =>
        logger.debug(t, "Failed mid-stream. Terminating stream, closing connection")
        exceptionStatsHandler.record(statsReceiver.scope("stream"), t)
        rep.reader.discard()
      }
      p.setInterruptHandler {
        case intr =>
          rep.reader.discard()
          f.raise(intr)
      }
      p
    } else {
      trans.write(rep)
    }
  }

  /**
   * Set the Connection header as appropriate. This will NOT clobber a 'Connection: close' header,
   * allowing services to gracefully close the connection through the Connection header mechanism.
   */
  private def setKeepAlive(rep: Response, keepAlive: Boolean): Unit = {
    val connectionHeaders = rep.headerMap.getAll(Fields.Connection)
    if (connectionHeaders.isEmpty || !connectionHeaders.exists("close".equalsIgnoreCase(_))) {
      rep.version match {
        case Version.Http10 if keepAlive =>
          rep.headerMap.setUnsafe(Fields.Connection, "keep-alive")

        case Version.Http11 if !keepAlive =>
          // The connection header may contain additional information, so add
          // rather than set.
          rep.headerMap.addUnsafe(Fields.Connection, "close")

        case _ =>
      }
    }
  }
}
