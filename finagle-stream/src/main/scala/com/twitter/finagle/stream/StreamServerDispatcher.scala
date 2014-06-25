package com.twitter.finagle.stream

import com.twitter.finagle.Service
import com.twitter.finagle.transport.Transport
import org.jboss.netty.handler.codec.http._
import com.twitter.finagle.dispatch.GenSerialServerDispatcher
import com.twitter.util.{Future, Promise}

/**
 * Stream StreamResponse messages into HTTP chunks.
 */
private class StreamServerDispatcher(
    trans: Transport[Any, Any], 
    service: Service[HttpRequest, StreamResponse])
  extends GenSerialServerDispatcher[HttpRequest, StreamResponse, Any, Any](trans) {

  trans.onClose ensure {
    service.close()
  }

  private[this] def writeChunks(rep: StreamResponse): Future[Unit] = {
    (rep.messages or rep.error).sync() flatMap {
      case Left(bytes) =>
        trans.write(new DefaultHttpChunk(bytes)) flatMap { unit => writeChunks(rep) }
      case Right(exc) =>
        trans.write(new DefaultHttpChunkTrailer {
          override def isLast(): Boolean = 
            rep.httpResponse.isChunked
        })
    }
  }
  
  protected def dispatch(req: Any, eos: Promise[Unit]) = req match {
    case req: HttpRequest =>
      service(req) ensure eos.setDone()
    case invalid =>
      eos.setDone()
      Future.exception(new IllegalArgumentException("Invalid message "+invalid))
  }
  
  protected def handle(rep: StreamResponse) = {
    // Note: It's rather bad to modify the response here like we do,
    // it certainly violates what we do elsewhere in Finagle. However,
    // it is difficult to do this right within the context of the current 
    // Netty HTTP abstractions.
    val httpRes = rep.httpResponse
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

    trans.write(httpRes) flatMap { unit => 
      writeChunks(rep) 
    } ensure {
      rep.release()
      trans.close()
    }
  }
}
