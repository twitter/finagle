package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.{Fields, HeaderMap}
import com.twitter.finagle.netty4.{ByteBufAsBuf, BufAsByteBuf}
import com.twitter.finagle.{http => FinagleHttp}
import com.twitter.io.{BufReader, Reader}
import io.netty.handler.codec.{http => NettyHttp}
import java.net.InetSocketAddress

private[finagle] object Bijections {

  object netty {
    def headersToFinagle(headers: NettyHttp.HttpHeaders): FinagleHttp.HeaderMap =
      new Netty4HeaderMap(headers)

    def versionToFinagle(v: NettyHttp.HttpVersion): FinagleHttp.Version = v match {
      case NettyHttp.HttpVersion.HTTP_1_0 => FinagleHttp.Version.Http10
      case NettyHttp.HttpVersion.HTTP_1_1 => FinagleHttp.Version.Http11
      case _ => FinagleHttp.Version.Http11
    }

    def methodToFinagle(m: NettyHttp.HttpMethod): FinagleHttp.Method =
      FinagleHttp.Method(m.name)

    def statusToFinagle(s: NettyHttp.HttpResponseStatus): FinagleHttp.Status =
      FinagleHttp.Status.fromCode(s.code)


    def chunkedRequestToFinagle(
      in: NettyHttp.HttpRequest,
      r: Reader,
      remoteAddr: InetSocketAddress
    ): FinagleHttp.Request = {

      val result =
        FinagleHttp.Request(
          version = Bijections.netty.versionToFinagle(in.protocolVersion),
          method = Bijections.netty.methodToFinagle(in.method),
          uri = in.uri,
          reader = r,
          remoteAddr = remoteAddr
        )

      result.setChunked(true)
      writeNettyHeadersToFinagle(in.headers, result.headerMap)
      result
    }

    private[this] def copyHeadersAndBody(nettyMsg: NettyHttp.HttpMessage, finMsg: FinagleHttp.Message): Unit = {
      writeNettyHeadersToFinagle(nettyMsg.headers, finMsg.headerMap)
      nettyMsg match {
        case hasContent: NettyHttp.HttpContent =>
          finMsg.content = ByteBufAsBuf.Owned(hasContent.content)
        case _ =>
      }
    }

    def fullRequestToFinagle(
      r: NettyHttp.FullHttpRequest,
      remoteAddr: InetSocketAddress
    ): FinagleHttp.Request = {

      val result = FinagleHttp.Request(
        method = methodToFinagle(r.method),
        uri = r.uri,
        version = versionToFinagle(r.protocolVersion),
        reader = BufReader(ByteBufAsBuf.Owned(r.content)),
        remoteAddr = remoteAddr
      )
      result.setChunked(false)
      copyHeadersAndBody(r, result)
      result
    }

    private[http] def writeNettyHeadersToFinagle(head: NettyHttp.HttpHeaders, out: HeaderMap): Unit = {
      val itr = head.iteratorAsString()
      while (itr.hasNext) {
        val entry = itr.next()
        out.add(entry.getKey, entry.getValue)
      }
    }

    def chunkedResponseToFinagle(in: NettyHttp.HttpResponse, r: Reader): FinagleHttp.Response = {
      val resp = FinagleHttp.Response(
        versionToFinagle(in.protocolVersion),
        statusToFinagle(in.status),
        reader = r
      )
      resp.setChunked(true)
      writeNettyHeadersToFinagle(in.headers, resp.headerMap)
      resp
    }

    def fullResponseToFinagle(rep: NettyHttp.FullHttpResponse): FinagleHttp.Response = {
      val resp = FinagleHttp.Response(
        versionToFinagle(rep.protocolVersion),
        statusToFinagle(rep.status),
        BufReader(ByteBufAsBuf.Owned(rep.content))
      )
      resp.setChunked(false)
      copyHeadersAndBody(rep, resp)
      resp
    }
  }

  object finagle {

    def headersToNetty(h: FinagleHttp.HeaderMap): NettyHttp.HttpHeaders = h match {
      case map: Netty4HeaderMap =>
        map.underlying

      case _ =>
        // We don't want to validate headers here since they are already validated
        // by Netty 3 DefaultHttpHeaders. This not only allows us to be efficient
        // but also preserves the behavior of Netty 3.
        val result = new NettyHttp.DefaultHttpHeaders(false/*validate headers*/)
        h.foreach { case (k,v) =>
          result.add(k, v)
        }
        result
    }

    def statusToNetty(s: FinagleHttp.Status): NettyHttp.HttpResponseStatus =
      NettyHttp.HttpResponseStatus.valueOf(s.code)

    def versionToNetty(v: FinagleHttp.Version): NettyHttp.HttpVersion = v match {
      case FinagleHttp.Version.Http10 => NettyHttp.HttpVersion.HTTP_1_0
      case FinagleHttp.Version.Http11 => NettyHttp.HttpVersion.HTTP_1_1
      case _ => NettyHttp.HttpVersion.HTTP_1_1
    }

    def responseHeadersToNetty(r: FinagleHttp.Response): NettyHttp.HttpResponse =
      new NettyHttp.DefaultHttpResponse(
        versionToNetty(r.version),
        statusToNetty(r.status),
        headersToNetty(r.headerMap)
      )

    def fullResponseToNetty(r: FinagleHttp.Response): NettyHttp.HttpResponse =
      new NettyHttp.DefaultFullHttpResponse(
        versionToNetty(r.version),
        statusToNetty(r.status),
        BufAsByteBuf.Owned(r.content),
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
          result.headers.add(
            NettyHttp.HttpHeaderNames.TRANSFER_ENCODING, NettyHttp.HttpHeaderValues.CHUNKED)
        }
        result
      }
      else {
        new NettyHttp.DefaultFullHttpRequest(
          versionToNetty(r.version),
          methodToNetty(r.method),
          r.uri,
          BufAsByteBuf.Owned(r.content),
          headersToNetty(r.headerMap),
          NettyHttp.EmptyHttpHeaders.INSTANCE // finagle-http doesn't support trailing headers
        )

      }
    }
  }
}
