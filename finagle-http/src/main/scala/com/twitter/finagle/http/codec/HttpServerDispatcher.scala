package com.twitter.finagle.http.codec

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.finagle.http._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.stats.{StatsReceiver, DefaultStatsReceiver, RollupStatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.Throwables
import com.twitter.io.{Reader, Buf, BufReader}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Throw, Return, NonFatal}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.frame.TooLongFrameException
import org.jboss.netty.handler.codec.http.{HttpRequest=>HttpAsk, _}

class HttpServerDispatcher[ASK <: Ask](
    trans: Transport[Any, Any],
    service: Service[ASK, HttpResponse],
    stats: StatsReceiver)
  extends GenSerialServerDispatcher[ASK, HttpResponse, Any, Any](trans) {

  def this(
    trans: Transport[Any, Any],
    service: Service[ASK, HttpResponse]) = this(trans, service, DefaultStatsReceiver)

  private[this] val failureReceiver = new RollupStatsReceiver(stats.scope("stream")).scope("failures")

  import ReaderUtils.{readChunk, streamChunks}

  trans.onClose ensure {
    service.close()
  }

  private[this] def BadAskResponse =
    Response(HttpVersion.HTTP_1_0, HttpResponseStatus.BAD_REQUEST)

  private[this] def AskUriTooLongResponse =
    Response(HttpVersion.HTTP_1_0, HttpResponseStatus.REQUEST_URI_TOO_LONG)

  private[this] def AskHeaderFieldsTooLarge =
    Response(HttpVersion.HTTP_1_0, HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE)

  protected def dispatch(m: Any, eos: Promise[Unit]) = m match {
    case badReq: BadHttpAsk =>
      eos.setDone()
      val response = badReq.exception match {
        case ex: TooLongFrameException =>
          // this is very brittle :(
          if (ex.getMessage().startsWith("An HTTP line is larger than "))
            AskUriTooLongResponse
          else
            AskHeaderFieldsTooLarge
        case _ =>
          BadAskResponse
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

    case reqIn: HttpAsk =>
      val req = new Ask {
        val httpAsk = reqIn
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
      }.asInstanceOf[ASK]

      service(req)

    case invalid =>
      eos.setDone()
      Future.exception(new IllegalArgumentException("Invalid message "+invalid))
  }

  protected def handle(response: HttpResponse): Future[Unit] = {
    HttpHeaders.setKeepAlive(response, !isClosing)
    response match {
      case rep: Response if rep.isChunked =>
        // We remove content length here in case the content is later
        // compressed. This is a pretty bad violation of modularity:
        // this is likely an issue with the Netty content
        // compressors, which (should?) adjust headers regardless of
        // transfer encoding.
        rep.headers.remove(HttpHeaders.Names.CONTENT_LENGTH)

        val p = new Promise[Unit]
        val f = trans.write(rep) before streamChunks(trans, rep.reader)
        // This awkwardness is unfortunate but necessary for now as you may be
        // interrupted in the middle of a write, or when there otherwise isn’t
        // an outstanding read (e.g. read-write race).
        p.become(f onFailure { case t: Throwable =>
          Logger.get(this.getClass.getName).debug(t, "Failed mid-stream. Terminating stream, closing connection")
          failureReceiver.counter(Throwables.mkString(t): _*).incr()
          rep.reader.discard()
        })
        p setInterruptHandler { case _ => rep.reader.discard() }
        p
      case rep: Response =>
        // Ensure Content-Length is set if not chunked
        if (!rep.headers.contains(HttpHeaders.Names.CONTENT_LENGTH))
          rep.contentLength = rep.getContent().readableBytes

        trans.write(rep)
      case _ =>
        // Ensure Content-Length is set if not chunked
        if (!response.isChunked && !HttpHeaders.isContentLengthSet(response))
          HttpHeaders.setContentLength(response, response.getContent().readableBytes)

        trans.write(response)
    }
  }
}
