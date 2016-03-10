package com.twitter.finagle.http4

import com.twitter.finagle.http.HeaderMap
import com.twitter.finagle.netty4.{ByteBufAsBuf, BufAsByteBuf}
import com.twitter.finagle.{http => FinagleHttp}
import io.netty.handler.codec.{http => NettyHttp}

private[http4] object Bijections {

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

    def requestToFinagle(r: NettyHttp.FullHttpRequest): FinagleHttp.Request = {
      val result = FinagleHttp.Request(
        method = methodToFinagle(r.method),
        uri = r.uri,
        version = versionToFinagle(r.protocolVersion)
      )
      writeNettyHeadersToFinagle(r.headers, result.headerMap)
      result.content = ByteBufAsBuf.Owned(r.content)
      result
    }

    private[this] def writeNettyHeadersToFinagle(head: NettyHttp.HttpHeaders, out: HeaderMap): Unit = {
      val itr = head.iteratorAsString()
      while (itr.hasNext) {
        val entry = itr.next()
        out.add(entry.getKey, entry.getValue)
      }
    }

    def responseToFinagle(rep: NettyHttp.HttpResponse): FinagleHttp.Response = rep match {
      case full: NettyHttp.FullHttpResponse =>
        val resp = FinagleHttp.Response(
          versionToFinagle(rep.protocolVersion),
          statusToFinagle(rep.status)
        )
        writeNettyHeadersToFinagle(rep.headers, resp.headerMap)
        resp.content = ByteBufAsBuf.Owned(full.content)

        resp

      // note: HttpContent chunks are handled in the dispatcher
      case invalid =>
        throw new IllegalArgumentException("unexpected response type: " + invalid.toString)
    }
  }

  object finagle {

    def headersToNetty(h: FinagleHttp.HeaderMap): NettyHttp.HttpHeaders = h match {
      case map: Netty4HeaderMap =>
        map.underlying

      case _ =>
        val result = new NettyHttp.DefaultHttpHeaders()
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
    }

    def responseToNetty(r: FinagleHttp.Response): NettyHttp.FullHttpResponse =
      new NettyHttp.DefaultFullHttpResponse(
        versionToNetty(r.version),
        statusToNetty(r.status),
        BufAsByteBuf.Owned(r.content),
        headersToNetty(r.headerMap),
        NettyHttp.EmptyHttpHeaders.INSTANCE // only chunked messages have trailing headers
      )

    def methodToNetty(m: FinagleHttp.Method): NettyHttp.HttpMethod =
      NettyHttp.HttpMethod.valueOf(m.toString)

    def requestToNetty(r: FinagleHttp.Request): NettyHttp.FullHttpRequest =
      new NettyHttp.DefaultFullHttpRequest(
        versionToNetty(r.version),
        methodToNetty(r.method),
        r.uri,
        BufAsByteBuf.Owned(r.content),
        headersToNetty(r.headerMap),
        NettyHttp.EmptyHttpHeaders.INSTANCE // only chunked messages have trailing headers
      )
  }
}
