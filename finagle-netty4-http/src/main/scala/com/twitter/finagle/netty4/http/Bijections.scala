package com.twitter.finagle.netty4.http

import com.twitter.app.GlobalFlag
import com.twitter.finagle.http.{Fields, HeaderMap, Request}
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.finagle.{http => FinagleHttp}
import com.twitter.io.{Buf, Reader}
import io.netty.handler.codec.{http => NettyHttp}
import java.net.InetSocketAddress

object revalidateInboundHeaders
    extends GlobalFlag[Boolean](
      default = false,
      help = "Perform Finagle based validation of headers when converting from Netty `HeaderMap`s"
    )

private[finagle] object Bijections {

  object netty {
    def versionToFinagle(v: NettyHttp.HttpVersion): FinagleHttp.Version = v match {
      case NettyHttp.HttpVersion.HTTP_1_0 => FinagleHttp.Version.Http10
      case NettyHttp.HttpVersion.HTTP_1_1 => FinagleHttp.Version.Http11
      case _ => FinagleHttp.Version.Http11
    }

    def methodToFinagle(m: NettyHttp.HttpMethod): FinagleHttp.Method =
      FinagleHttp.Method(m.name)

    def statusToFinagle(s: NettyHttp.HttpResponseStatus): FinagleHttp.Status =
      FinagleHttp.Status.fromCode(s.code)

    private def requestToFinagleHelper(
      in: NettyHttp.HttpRequest,
      r: Reader[Buf],
      remoteAddr: InetSocketAddress,
      chunked: Boolean
    ): Request = {
      val result = new Request.Impl(r, remoteAddr)

      result.setChunked(chunked)
      result.version = Bijections.netty.versionToFinagle(in.protocolVersion)
      result.method = Bijections.netty.methodToFinagle(in.method)
      result.uri = in.uri

      writeNettyHeadersToFinagle(in.headers, result.headerMap)

      result
    }

    def chunkedRequestToFinagle(
      in: NettyHttp.HttpRequest,
      r: Reader[Buf],
      remoteAddr: InetSocketAddress
    ): FinagleHttp.Request = requestToFinagleHelper(in, r, remoteAddr, chunked = true)

    def fullRequestToFinagle(
      in: NettyHttp.FullHttpRequest,
      remoteAddr: InetSocketAddress
    ): FinagleHttp.Request = {
      val payload = ByteBufConversion.byteBufAsBuf(in.content)
      val reader = Reader.fromBuf(payload)
      val result = requestToFinagleHelper(in, reader, remoteAddr, chunked = false)
      result.content = payload

      result
    }

    def headersToFinagle(h: NettyHttp.HttpHeaders): FinagleHttp.HeaderMap = {
      val result = FinagleHttp.HeaderMap.newHeaderMap
      writeNettyHeadersToFinagle(h, result)

      result
    }

    def writeNettyHeadersToFinagle(head: NettyHttp.HttpHeaders, out: HeaderMap): Unit = {
      val shouldValidate = revalidateInboundHeaders()
      val itr = head.iteratorAsString()
      while (itr.hasNext) {
        val entry = itr.next()
        // addUnsafe because Netty already validates Headers for us, but sometimes
        // it's better to be double sure so enable opting into revalidation.
        if (shouldValidate) out.add(entry.getKey, entry.getValue)
        else out.addUnsafe(entry.getKey, entry.getValue)
      }
    }

    private def responseToFinagleHelper(
      in: NettyHttp.HttpResponse,
      r: Reader[Buf],
      chunked: Boolean
    ): FinagleHttp.Response = {
      val result = new FinagleHttp.Response.Impl(r)

      result.setChunked(chunked)
      result.version = versionToFinagle(in.protocolVersion())
      result.status = statusToFinagle(in.status)

      writeNettyHeadersToFinagle(in.headers, result.headerMap)

      result
    }

    def chunkedResponseToFinagle(in: NettyHttp.HttpResponse, r: Reader[Buf]): FinagleHttp.Response =
      responseToFinagleHelper(in, r, chunked = true)

    def fullResponseToFinagle(in: NettyHttp.FullHttpResponse): FinagleHttp.Response = {
      val payload = ByteBufConversion.byteBufAsBuf(in.content)
      val reader = Reader.fromBuf(payload)

      val result = responseToFinagleHelper(in, reader, chunked = false)
      result.content = payload

      result
    }
  }

  object finagle {

    def writeFinagleHeadersToNetty(in: FinagleHttp.HeaderMap, out: NettyHttp.HttpHeaders): Unit = {
      in.foreach {
        case (k, v) =>
          out.add(k, v)
      }
    }

    def headersToNetty(h: FinagleHttp.HeaderMap): NettyHttp.HttpHeaders = {
      // We don't want to validate headers here since they are already validated
      // by Finagle's own HeaderMap.
      val result = new NettyHttp.DefaultHttpHeaders(false /*validate headers*/ )
      writeFinagleHeadersToNetty(h, result)
      result
    }

    def statusToNetty(s: FinagleHttp.Status): NettyHttp.HttpResponseStatus =
      NettyHttp.HttpResponseStatus.valueOf(s.code)

    def versionToNetty(v: FinagleHttp.Version): NettyHttp.HttpVersion = v match {
      case FinagleHttp.Version.Http10 => NettyHttp.HttpVersion.HTTP_1_0
      case FinagleHttp.Version.Http11 => NettyHttp.HttpVersion.HTTP_1_1
      case _ => NettyHttp.HttpVersion.HTTP_1_1
    }

    def chunkedResponseToNetty(r: FinagleHttp.Response): NettyHttp.HttpResponse =
      new NettyHttp.DefaultHttpResponse(
        versionToNetty(r.version),
        statusToNetty(r.status),
        headersToNetty(r.headerMap)
      )

    def fullResponseToNetty(r: FinagleHttp.Response): NettyHttp.FullHttpResponse =
      new NettyHttp.DefaultFullHttpResponse(
        versionToNetty(r.version),
        statusToNetty(r.status),
        ByteBufConversion.bufAsByteBuf(r.content),
        headersToNetty(r.headerMap),
        NettyHttp.EmptyHttpHeaders.INSTANCE // only chunked messages have trailing headers
      )

    def methodToNetty(m: FinagleHttp.Method): NettyHttp.HttpMethod =
      NettyHttp.HttpMethod.valueOf(m.toString)

    def requestToNetty(r: FinagleHttp.Request): NettyHttp.HttpRequest = {
      if (r.isChunked) {
        val result = new NettyHttp.DefaultHttpRequest(
          versionToNetty(r.version),
          methodToNetty(r.method),
          r.uri,
          headersToNetty(r.headerMap)
        )
        // We only set the Transfer-Encoding to "chunked" if the request does not have
        // Content-Length set. This mimics Netty 3 behavior, wherein a request can be "chunked"
        // and not have a "Transfer-Encoding: chunked" header (instead, it has a Content-Length).
        if (!r.headerMap.contains(Fields.ContentLength)) {
          result.headers
            .add(NettyHttp.HttpHeaderNames.TRANSFER_ENCODING, NettyHttp.HttpHeaderValues.CHUNKED)
        }
        result
      } else {
        new NettyHttp.DefaultFullHttpRequest(
          versionToNetty(r.version),
          methodToNetty(r.method),
          r.uri,
          ByteBufConversion.bufAsByteBuf(r.content),
          headersToNetty(r.headerMap),
          NettyHttp.EmptyHttpHeaders.INSTANCE // finagle-http doesn't support trailing headers
        )

      }
    }
  }
}
