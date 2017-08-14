package com.twitter.finagle.http.netty

import com.twitter.finagle.dispatch.GenSerialClientDispatcher.wrapWriteException
import com.twitter.finagle.http._
import com.twitter.finagle.http.ReaderUtils.{readChunk, streamChunks}
import com.twitter.finagle.http.exp.{Multi, StreamTransportProxy}
import com.twitter.finagle.http.netty.Bijections._
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.transport.Transport
import com.twitter.io.{BufReader, Reader}
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.jboss.netty.handler.codec.http.{HttpMessage, HttpRequest, HttpResponse}

private[finagle] class Netty3StreamTransport[
  In <: Message,
  Out <: Message,
  NettyIn <: HttpMessage,
  NettyOut <: HttpMessage: Manifest
](rawTransport: Transport[Any, Any], mkMessage: (NettyOut, Reader) => Out)(
  implicit injection: Injection[In, NettyIn]
) extends StreamTransportProxy[In, Out](rawTransport) {

  private[this] val transport = Transport.cast[NettyIn, NettyOut](rawTransport)
  private[this] val readFn: NettyOut => Future[Multi[Out]] = {
    case res if !res.isChunked =>
      val reader = BufReader(ChannelBufferBuf.Owned(res.getContent))
      Future.value(Multi(mkMessage(res, reader), Future.Done))
    case res =>
      val coll: Reader with Future[Unit] = Transport.collate(rawTransport, readChunk)
      Future.value(Multi(mkMessage(res, coll), coll))
  }

  def write(msg: In): Future[Unit] =
    transport.write(Bijections.from[In, NettyIn](msg)).rescue(wrapWriteException).before {
      if (msg.isChunked) streamChunks(rawTransport, msg.reader) else Future.Done
    }

  def read(): Future[Multi[Out]] = transport.read().flatMap(readFn)
}

private[finagle] class Netty3ClientStreamTransport(transport: Transport[Any, Any])
    extends Netty3StreamTransport[Request, Response, HttpRequest, HttpResponse](
      transport, {
        case (resp: HttpResponse, readerIn: Reader) =>
          val finagleResp = Response.chunked(
            Bijections.from(resp.getProtocolVersion),
            Bijections.from(resp.getStatus),
            readerIn
          )
          if (!resp.isChunked) {
            finagleResp.setChunked(false)
          }
          Bijections.copyHeadersAndContentFromNetty(resp, finagleResp)
          finagleResp
      }
    )

private[finagle] class Netty3ServerStreamTransport(transport: Transport[Any, Any])
    extends Netty3StreamTransport[Response, Request, HttpResponse, HttpRequest](
      transport, {
        case (req: HttpRequest, readerIn: Reader) =>
          val remoteSocketAddress = transport.remoteAddress match {
            case ia: InetSocketAddress => ia
            case _ => new InetSocketAddress(0)
          }

          val finagleRequest = Request.chunked(
            Bijections.from(req.getProtocolVersion),
            Bijections.from(req.getMethod),
            req.getUri,
            readerIn,
            remoteSocketAddress
          )

          if (!req.isChunked) {
            finagleRequest.setChunked(false)
          }

          Bijections.copyHeadersAndContentFromNetty(req, finagleRequest)

          finagleRequest
      }
    )
