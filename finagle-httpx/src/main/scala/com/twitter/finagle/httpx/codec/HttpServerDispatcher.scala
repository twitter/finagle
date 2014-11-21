package com.twitter.finagle.httpx.codec

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.finagle.httpx._
import com.twitter.finagle.httpx.netty.Bijections._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.stats.{StatsReceiver, DefaultStatsReceiver, RollupStatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.Throwables
import com.twitter.io.{Reader, Buf, BufReader}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Throw, Return, NonFatal}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpResponse, HttpHeaders}

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

  private[this] def BadRequestResponse =
    Response(Version.Http10, Status.BadRequest)

  private[this] def RequestUriTooLongResponse =
    Response(Version.Http10, Status.RequestURITooLong)

  private[this] def RequestHeaderFieldsTooLarge =
    Response(Version.Http10, Status.RequestHeaderFieldsTooLarge)

  protected def dispatch(m: Any, eos: Promise[Unit]) = m match {
    case badReq: BadHttpRequest =>
      eos.setDone()
      val response = badReq.exception match {
        case ex: TooLongFrameException =>
          // this is very brittle :(
          if (ex.getMessage().startsWith("An HTTP line is larger than "))
            RequestUriTooLongResponse
          else
            RequestHeaderFieldsTooLarge
        case _ =>
          BadRequestResponse
      }
      // The connection in unusable, close it
      HttpHeaders.setKeepAlive(response.httpResponse, false)
      Future.value(response)

    case reqIn: HttpRequest =>
      val req = new Request {
        val httpRequest = reqIn
        override val httpMessage = reqIn
        lazy val remoteSocketAddress = trans.remoteAddress match {
          case ia: InetSocketAddress => ia
          case _ => new InetSocketAddress(0)
        }

        override val reader =
          if (reqIn.isChunked) {
            val coll = Transport.collate(trans, readChunk)
            coll.proxyTo(eos)
            coll: Reader
          } else {
            eos.setDone()
            BufReader(ChannelBufferBuf.Owned(reqIn.getContent))
          }

      }

      service(req)

    case invalid =>
      eos.setDone()
      Future.exception(new IllegalArgumentException("Invalid message "+invalid))
  }

  protected def handle(rep: Response): Future[Unit] = {
    if (isClosing) rep.headers.set(Fields.Connection, "close")
    if (rep.isChunked) {
      // We remove content length here in case the content is later
      // compressed. This is a pretty bad violation of modularity:
      // this is likely an issue with the Netty content
      // compressors, which (should?) adjust headers regardless of
      // transfer encoding.
      rep.headers.remove(HttpHeaders.Names.CONTENT_LENGTH)

      val p = new Promise[Unit]
      val f = trans.write(from[Response, HttpResponse](rep)) before
        streamChunks(trans, rep.reader)
      // This awkwardness is unfortunate but necessary for now as you may be
      // interrupted in the middle of a write, or when there otherwise isn’t
      // an outstanding read (e.g. read-write race).
      p.become(f onFailure { case t: Throwable =>
        Logger.get(this.getClass.getName).debug(t, "Failed mid-stream. Terminating stream, closing connection")
        failureReceiver.counter(Throwables.mkString(t): _*).incr()
        rep.reader.discard()
      })
      p setInterruptHandler { case intr => rep.reader.discard() }
      p
    } else {
      // Ensure Content-Length is set if not chunked
      if (!rep.headers.contains(Fields.ContentLength))
        rep.contentLength = rep.content.length

      trans.write(from[Response, HttpResponse](rep))
    }
  }
}
