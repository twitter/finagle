package com.twitter.finagle.stream

import com.twitter.finagle.Service
import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.finagle.netty3.BufChannelBuffer
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise}
import org.jboss.netty.handler.codec.http._

/**
 * Stream StreamResponse messages into HTTP chunks.
 */
private[twitter] class StreamServerDispatcher[Req: RequestType](
    trans: Transport[Any, Any],
    service: Service[Req, StreamResponse]
) extends GenSerialServerDispatcher[Req, StreamResponse, Any, Any](trans) {
  import Bijections._

  trans.onClose ensure {
    service.close()
  }

  private[this] val RT = implicitly[RequestType[Req]]

  private[this] def writeChunks(rep: StreamResponse): Future[Unit] = {
    (rep.messages or rep.error).sync() flatMap {
      case Left(buf) =>
        val bytes = BufChannelBuffer(buf)
        trans.write(new DefaultHttpChunk(bytes)) before writeChunks(rep)
      case Right(exc) =>
        trans.write(HttpChunk.LAST_CHUNK)
    }
  }

  protected def dispatch(req: Any, eos: Promise[Unit]) = req match {
    case httpReq: HttpRequest =>
      service(RT.specialize(from(httpReq))) ensure eos.setDone()
    case invalid =>
      eos.setDone()
      Future.exception(new IllegalArgumentException(s"Invalid message: $invalid"))
  }

  protected def handle(rep: StreamResponse) = {
    val httpRes: HttpResponse = from(rep.info)

    httpRes.setChunked(
      httpRes.getProtocolVersion == HttpVersion.HTTP_1_1 &&
      httpRes.headers.get(HttpHeaders.Names.CONTENT_LENGTH) == null)

    if (httpRes.isChunked) {
      HttpHeaders.setHeader(
        httpRes, HttpHeaders.Names.TRANSFER_ENCODING,
        HttpHeaders.Values.CHUNKED)
    } else {
      HttpHeaders.setHeader(
        httpRes, HttpHeaders.Names.CONNECTION,
        HttpHeaders.Values.CLOSE)
    }

    val f = trans.write(httpRes).before {
      writeChunks(rep)
    }.ensure {
      rep.release()
      trans.close()
    }

    val p = new Promise[Unit]()
    f.proxyTo(p)
    p.setInterruptHandler { case _ => rep.release() }
    p
  }

}
