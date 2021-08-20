package com.twitter.finagle.netty4.http

import com.twitter.app.GlobalFlag
import com.twitter.finagle.http.{
  Chunk,
  Fields,
  HeaderMap,
  Method,
  Request,
  Response,
  Status,
  Version
}
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.io.Reader
import io.netty.handler.codec.{http => NettyHttp}
import java.net.InetSocketAddress

object revalidateInboundHeaders
    extends GlobalFlag[Boolean](
      default = false,
      help = "Perform Finagle based validation of headers when converting from Netty `HeaderMap`s"
    )

private[finagle] object Bijections {

  object netty {
    def versionToFinagle(v: NettyHttp.HttpVersion): Version = v match {
      case NettyHttp.HttpVersion.HTTP_1_0 => Version.Http10
      case NettyHttp.HttpVersion.HTTP_1_1 => Version.Http11
      case _ => Version.Http11
    }

    def methodToFinagle(m: NettyHttp.HttpMethod): Method =
      Method(m.name)

    def statusToFinagle(s: NettyHttp.HttpResponseStatus): Status =
      Status.fromCode(s.code)

    private def copyToFinagleRequest(in: NettyHttp.HttpRequest, out: Request): Unit = {
      out.version = Bijections.netty.versionToFinagle(in.protocolVersion)
      out.method = Bijections.netty.methodToFinagle(in.method)
      out.uri = in.uri

      writeNettyHeadersToFinagle(in.headers, out.headerMap)
    }

    def chunkedRequestToFinagle(
      in: NettyHttp.HttpRequest,
      r: Reader[Chunk],
      remoteAddr: InetSocketAddress
    ): Request = {
      val out = new Request.Inbound(r, remoteAddr, HeaderMap.Empty)
      out.setChunked(true)
      copyToFinagleRequest(in, out)

      out
    }

    def fullRequestToFinagle(
      in: NettyHttp.FullHttpRequest,
      remoteAddr: InetSocketAddress
    ): Request = {
      val payload = ByteBufConversion.byteBufAsBuf(in.content)

      val reader =
        if (payload.isEmpty) Reader.empty[Chunk]
        else Reader.value(Chunk(payload))

      val trailers =
        if (in.trailingHeaders.isEmpty) HeaderMap.Empty
        else headersToFinagle(in.trailingHeaders)

      val out = new Request.Inbound(reader, remoteAddr, trailers)

      out.setChunked(false)
      out.content = payload

      copyToFinagleRequest(in, out)

      out
    }

    def headersToFinagle(h: NettyHttp.HttpHeaders): HeaderMap = {
      val result = HeaderMap.newHeaderMap
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

    private def copyToFinagleResponse(in: NettyHttp.HttpResponse, out: Response): Unit = {
      out.version = versionToFinagle(in.protocolVersion())
      out.status = statusToFinagle(in.status)

      writeNettyHeadersToFinagle(in.headers, out.headerMap)
    }

    def chunkedResponseToFinagle(in: NettyHttp.HttpResponse, r: Reader[Chunk]): Response = {
      val out = new Response.Inbound(r, HeaderMap.Empty)
      out.setChunked(true)
      copyToFinagleResponse(in, out)

      out
    }

    def fullResponseToFinagle(in: NettyHttp.FullHttpResponse): Response = {
      val payload = ByteBufConversion.byteBufAsBuf(in.content)

      val reader =
        if (payload.isEmpty) Reader.empty[Chunk]
        else Reader.value(Chunk(payload))

      val trailers =
        if (in.trailingHeaders.isEmpty) HeaderMap.Empty
        else headersToFinagle(in.trailingHeaders)

      val out = new Response.Inbound(reader, trailers)

      out.setChunked(false)
      out.content = payload

      copyToFinagleResponse(in, out)

      out
    }
  }

  object finagle {

    def writeFinagleHeadersToNetty(in: HeaderMap, out: NettyHttp.HttpHeaders): Unit =
      in.nameValueIterator.foreach { nv => out.add(nv.name, nv.value) }

    def headersToNetty(h: HeaderMap): NettyHttp.HttpHeaders = {
      // We don't want to validate headers here since they are already validated
      // by Finagle's own HeaderMap.
      val result = new NettyHttp.DefaultHttpHeaders(false /*validate headers*/ )
      writeFinagleHeadersToNetty(h, result)
      result
    }

    def statusToNetty(s: Status): NettyHttp.HttpResponseStatus =
      NettyHttp.HttpResponseStatus.valueOf(s.code)

    def versionToNetty(v: Version): NettyHttp.HttpVersion = v match {
      case Version.Http10 => NettyHttp.HttpVersion.HTTP_1_0
      case Version.Http11 => NettyHttp.HttpVersion.HTTP_1_1
      case _ => NettyHttp.HttpVersion.HTTP_1_1
    }

    def chunkedResponseToNetty(r: Response): NettyHttp.HttpResponse =
      new NettyHttp.DefaultHttpResponse(
        versionToNetty(r.version),
        statusToNetty(r.status),
        headersToNetty(r.headerMap)
      )

    def fullResponseToNetty(r: Response): NettyHttp.FullHttpResponse =
      new NettyHttp.DefaultFullHttpResponse(
        versionToNetty(r.version),
        statusToNetty(r.status),
        ByteBufConversion.bufAsByteBuf(r.content),
        headersToNetty(r.headerMap),
        NettyHttp.EmptyHttpHeaders.INSTANCE // trailers are only propagated from chunked messages
      )

    def methodToNetty(m: Method): NettyHttp.HttpMethod =
      NettyHttp.HttpMethod.valueOf(m.toString)

    def requestToNetty(r: Request, contentLengthHeader: Option[Long]): NettyHttp.HttpRequest = {
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
        } else {
          // Make sure we don't have a `Transfer-Encoding: chunked` header and `Content-Length` headers
          result.headers.remove(NettyHttp.HttpHeaderNames.TRANSFER_ENCODING)
        }
        result
      } else {
        val result = new NettyHttp.DefaultFullHttpRequest(
          versionToNetty(r.version),
          methodToNetty(r.method),
          r.uri,
          ByteBufConversion.bufAsByteBuf(r.content),
          headersToNetty(r.headerMap),
          NettyHttp.EmptyHttpHeaders.INSTANCE // trailers are only propagated from chunked messages
        )

        if (contentLengthHeader.isDefined) {
          result.headers.remove(NettyHttp.HttpHeaderNames.TRANSFER_ENCODING)
        }

        val realLength = r.content.length
        // see https://tools.ietf.org/html/rfc7230#section-3.3.2
        contentLengthHeader match {
          case Some(l) if realLength != l =>
            // need to clean up the content length header
            result.headers.set(NettyHttp.HttpHeaderNames.CONTENT_LENGTH, realLength.toString)

          case None if realLength > 0 =>
            // Set the content length if we are sure there is content.
            result.headers.set(NettyHttp.HttpHeaderNames.CONTENT_LENGTH, realLength.toString)

          case None if shouldHaveLengthHeader(r.method) =>
            // RFC 7230: "A user agent SHOULD send a Content-Length in a request message
            // when no Transfer-Encoding is sent and the request method defines a meaning
            // for an enclosed payload body."
            result.headers.set(NettyHttp.HttpHeaderNames.CONTENT_LENGTH, realLength.toString)

          case _ =>
          // NOP. Either the content length header already matches or
          // it doesn't exist for messages that should not have 0 value (see allowEmpty),
          // so there is nothing to do.
        }

        result
      }
    }

    private[this] def shouldHaveLengthHeader(method: Method): Boolean = {
      method match {
        case Method.Post | Method.Put | Method.Patch => true
        case _ => false
      }
    }
  }
}
