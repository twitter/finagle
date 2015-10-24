package com.twitter.finagle.http.codec

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.finagle.http._
import com.twitter.finagle.http.netty.Bijections._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.stats.{StatsReceiver, DefaultStatsReceiver, RollupStatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Reader, BufReader}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Throwables}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse}

class HttpServerDispatcher(
  trans: Transport[Any, Any],
  service: Service[Request, Response],
  stats: StatsReceiver) extends GenSerialServerDispatcher[Request, Response, Any, Any](trans) {

  def this(
    trans: Transport[Any, Any],
    service: Service[Request, Response]) = this(trans, service, DefaultStatsReceiver)

  private[this] val failureReceiver = new RollupStatsReceiver(stats.scope("stream")).scope("failures")

  import ReaderUtils.{readChunk, streamChunks}

  trans.onClose ensure {
    service.close()
  }

  protected def dispatch(m: Any, eos: Promise[Unit]): Future[Response] = m match {
    case badReq: BadHttpRequest =>
      eos.setDone()
      val response = badReq.exception match {
        case ex: TooLongFrameException =>
          // this is very brittle :(
          if (ex.getMessage().startsWith("An HTTP line is larger than "))
            Response(from(badReq.httpVersion), Status.RequestURITooLong)
          else
            Response(from(badReq.httpVersion), Status.RequestHeaderFieldsTooLarge)
        case _ =>
          Response(from(badReq.httpVersion), Status.BadRequest)
      }
      // The connection in unusable so we close it here.
      // Note that state != Idle while inside dispatch
      // so state will be set to Closed but trans.close
      // will not be called. Instead isClosing will be
      // set to true, keep-alive headers set correctly
      // in handle, and trans.close will be called in
      // the respond statement of loop().
      close()
      Future.value(response)

    case reqIn: HttpRequest =>
      val reader = if (reqIn.isChunked) {
        val coll = Transport.collate(trans, readChunk)
        coll.proxyTo(eos)
        coll: Reader
      } else {
        eos.setDone()
        BufReader(ChannelBufferBuf.Owned(reqIn.getContent))
      }

      val addr = trans.remoteAddress match {
        case ia: InetSocketAddress => ia
        case _ => new InetSocketAddress(0)
      }

      val req = Request(reqIn, reader, addr)
      service(req).handle {
        case _ => Response(req.version, Status.InternalServerError)
      }

    case invalid =>
      eos.setDone()
      Future.exception(new IllegalArgumentException("Invalid message "+invalid))
  }

  protected def handle(rep: Response): Future[Unit] = {
    setKeepAlive(rep, !isClosing)
    if (rep.isChunked) {
      // We remove content length here in case the content is later
      // compressed. This is a pretty bad violation of modularity:
      // this is likely an issue with the Netty content
      // compressors, which (should?) adjust headers regardless of
      // transfer encoding.
      rep.headers.remove(Fields.ContentLength)

      val p = new Promise[Unit]
      val f = trans.write(from[Response, HttpResponse](rep)) before
        streamChunks(trans, rep.reader)
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
      if (!rep.headers.contains(Fields.ContentLength))
        rep.contentLength = rep.content.length

      trans.write(from[Response, HttpResponse](rep))
    }
  }

  protected def setKeepAlive(rep: Response, keepAlive: Boolean): Unit = {
    rep.version match {
      case Version.Http10 =>
        if (keepAlive) {
          rep.headers.set(Fields.Connection, "keep-alive")
        } else {
          rep.headers.remove(Fields.Connection)
        }
      case Version.Http11 =>
        if (keepAlive) {
          rep.headers.remove(Fields.Connection)
        } else {
          rep.headers.set(Fields.Connection, "close")
        }
    }
  }
}
