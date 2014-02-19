package com.twitter.finagle.http.codec

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.finagle.http._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Reader, Buf}
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

class HttpServerDispatcher[REQUEST <: Request](
    trans: Transport[Any, Any],
    service: Service[REQUEST, HttpResponse])
  extends GenSerialServerDispatcher[REQUEST, HttpResponse, Any, Any](trans) {

  trans.onClose ensure {
    service.close()
  }

  private[this] def chunkOfBuf(buf: Buf): HttpChunk = buf match {
    case Buf.Eof =>
      HttpChunk.LAST_CHUNK
    case cb: ChannelBufferBuf => 
      new DefaultHttpChunk(cb.buf)
    case buf =>
      val bytes = new Array[Byte](buf.length)
      buf.write(bytes, 0)
      new DefaultHttpChunk(ChannelBuffers.wrappedBuffer(bytes))
  }

  private[this] def streamChunks(r: Reader): Future[Unit] = {
    r.read(Int.MaxValue) flatMap { buf =>
      val chunk = chunkOfBuf(buf)
      if (chunk.isLast) trans.write(chunk)
      else trans.write(chunk) before streamChunks(r)
    }
  }

  protected def dispatch(req: Any) = req match {
    case reqIn: HttpRequest if !reqIn.isChunked =>
      val req = new Request {
        val httpRequest = reqIn
        override val httpMessage = reqIn
        lazy val remoteSocketAddress = trans.remoteAddress match {
          case ia: InetSocketAddress => ia
          case _ => new InetSocketAddress(0)
        }
      }.asInstanceOf[REQUEST]
      
      service(req)

    case invalid =>
      Future.exception(new IllegalArgumentException("Invalid message "+invalid))
  }

  protected def handle(response: HttpResponse): Future[Unit] = response match {
    case rep: Response =>
      if (rep.isChunked) {
        trans.write(rep) before streamChunks(rep.reader)
      } else {
        // Ensure Content-Length is set if not chunked
        if (!rep.headers.contains(HttpHeaders.Names.CONTENT_LENGTH))
          rep.contentLength = rep.getContent().readableBytes

        trans.write(rep)
      }
    case _: HttpResponse =>
      trans.write(response)
  }
}
