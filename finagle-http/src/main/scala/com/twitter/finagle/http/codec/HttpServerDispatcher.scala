package com.twitter.finagle.http.codec

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.finagle.http._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Reader, Buf, BufReader}
import com.twitter.util.{Future, Promise, Throw, Return}
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http._

class HttpServerDispatcher[REQUEST <: Request](
    trans: Transport[Any, Any],
    service: Service[REQUEST, HttpResponse])
  extends GenSerialServerDispatcher[REQUEST, HttpResponse, Any, Any](trans) {

  import ReaderUtils.{readChunk, streamChunks}

  trans.onClose ensure {
    service.close()
  }

  protected def dispatch(m: Any, eos: Promise[Unit]) = m match {
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
            val readr = Reader.writable()
            Transport.copyToWriter(trans, readr)(readChunk) respond {
              case Throw(exc) => readr.fail(exc)
              case Return(_) => readr.close()
            } proxyTo(eos)
            readr
          } else {
            eos.setDone()
            BufReader(ChannelBufferBuf(reqIn.getContent))
          }

      }.asInstanceOf[REQUEST]

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
        p.become(f onFailure { _ => rep.reader.discard() })
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
