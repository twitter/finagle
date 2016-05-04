package com.twitter.finagle.netty4.http

import com.twitter.finagle.dispatch.GenSerialClientDispatcher.wrapWriteException
import com.twitter.finagle.http._
import com.twitter.finagle.http.exp.{Multi, StreamTransportProxy}
import com.twitter.finagle.netty4.{ByteBufAsBuf, BufAsByteBuf}
import com.twitter.finagle.transport.Transport
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, Return}
import io.netty.handler.codec.{http => NettyHttp, TooLongFrameException}

private[finagle] object StreamTransports {
  /**
   * used to collate http chunks off a raw netty-typed transport
   */
  def readChunk(chunk: Any): Future[Option[Buf]] = chunk match {
    case chunk: NettyHttp.LastHttpContent if chunk.content.readableBytes == 0 =>
      Future.None
    case chunk: NettyHttp.HttpContent =>
      Future.value(Some(ByteBufAsBuf.Owned(chunk.content)))

    case invalid =>
      Future.exception(
        new IllegalArgumentException("invalid message \"%s\"".format(invalid))
      )
  }
  def chunkOfBuf(buf: Buf): NettyHttp.HttpContent =
    new NettyHttp.DefaultHttpContent(BufAsByteBuf.Owned(buf))

  /**
   * read `bufSize` sized chunks off `trans` until it stops returning data.
   */
  def streamChunks(
    trans: Transport[Any, Any],
    r: Reader,
    // TODO Find a better number for bufSize, e.g. 32KiB - Buf overhead
    bufSize: Int = Int.MaxValue
  ): Future[Unit] = {
    r.read(bufSize).flatMap {
      case None =>
        trans.write(NettyHttp.LastHttpContent.EMPTY_LAST_CONTENT)
      case Some(buf) =>
        trans.write(chunkOfBuf(buf)).transform {
          case Return(_) => streamChunks(trans, r, bufSize)
          case _ => Future(r.discard())
        }
    }
  }

}

private[finagle] class Netty4ServerStreamTransport(
    rawTransport: Transport[Any, Any])
  extends StreamTransportProxy[Response, Request](rawTransport){
  import StreamTransports._

  private[this] val transport =
    Transport.cast[NettyHttp.HttpResponse, NettyHttp.HttpRequest](rawTransport)

  def write(in: Response): Future[Unit] = {
    val nettyRep =
      if (in.isChunked)
        Bijections.finagle.responseHeadersToNetty(in)
      else
        Bijections.finagle.fullResponseToNetty(in)

    transport.write(nettyRep).rescue(wrapWriteException).before {
      if (in.isChunked) streamChunks(rawTransport, in.reader)
      else Future.Done
    }
  }

  def read(): Future[Multi[Request]] = {
    transport.read().flatMap {
      case req: NettyHttp.FullHttpRequest if req.decoderResult.isFailure =>
        val exn = req.decoderResult.cause
        val bad = exn match {
          case ex: TooLongFrameException =>
            if (ex.getMessage.startsWith("An HTTP line is larger than "))
              BadRequest.uriTooLong(exn)
            else
              BadRequest.headerTooLong(exn)
          case _ =>
            BadRequest(exn)
        }
        Future.value(Multi(bad, Future.Done))

      case req: NettyHttp.FullHttpRequest =>
        // unchunked request
        val finagleReq: Request = Bijections.netty.requestToFinagle(req)
        Future.value(Multi(finagleReq, Future.Done))

      case req: NettyHttp.HttpRequest =>
        // chunked response
        val coll = Transport.collate(transport, readChunk)
        Future.value(
          Multi(
            Request(
              version = Bijections.netty.versionToFinagle(req.protocolVersion),
              method = Bijections.netty.methodToFinagle(req.method),
              uri = req.uri,
              reader = coll
            ),
            coll)
        )

      case invalid =>
        // relies on GenSerialClientDispatcher satisfying `p`
        Future.exception(new IllegalArgumentException(s"invalid message '$invalid'"))
    }
  }
}

private[finagle]class Netty4ClientStreamTransport(
    rawTransport: Transport[Any, Any])
  extends StreamTransportProxy[Request, Response](rawTransport){
  import StreamTransports._

  def write(in: Request): Future[Unit] = {
    val nettyReq = Bijections.finagle.requestToNetty(in)
    rawTransport.write(nettyReq).rescue(wrapWriteException).before {
      if (in.isChunked) streamChunks(rawTransport, in.reader)
      else Future.Done
    }
  }

  def read(): Future[Multi[Response]] = {
    rawTransport.read().flatMap {
      case rep: NettyHttp.FullHttpResponse =>
        // unchunked response
        val finagleRep: Response = Bijections.netty.responseToFinagle(rep)
        Future.value(Multi(finagleRep, Future.Done))

      case rep: NettyHttp.HttpResponse =>
        // start of chunked response
        val coll = Transport.collate(rawTransport, readChunk)
        Future.value(
          Multi(Response(
            Version.Http11, // chunked is necessarily 1.1
            Bijections.netty.statusToFinagle(rep.status),
            coll),
          coll)
        )

      case invalid =>
        // relies on GenSerialClientDispatcher satisfying `p`
        Future.exception(new IllegalArgumentException(s"invalid message '$invalid'"))
    }
  }
}
