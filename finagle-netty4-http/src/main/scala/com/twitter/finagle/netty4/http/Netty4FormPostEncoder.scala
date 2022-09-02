package com.twitter.finagle.netty4.http

import com.twitter.finagle.http._
import com.twitter.finagle.http.exp.FormPostEncoder
import com.twitter.finagle.netty4.ByteBufConversion
import com.twitter.io.Buf
import com.twitter.io.Reader
import io.netty.buffer.ByteBufAllocator
import io.netty.buffer.PooledByteBufAllocator
import io.netty.buffer.UnpooledByteBufAllocator
import io.netty.handler.codec.http.multipart.DefaultHttpDataFactory
import io.netty.handler.codec.http.multipart.HttpDataFactory
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder
import io.netty.handler.codec.http.DefaultHttpRequest
import io.netty.handler.codec.http.FullHttpRequest
import io.netty.handler.codec.http.HttpRequest
import io.netty.handler.codec.http.HttpUtil
import java.net.InetSocketAddress

private[finagle] object Netty4FormPostEncoder extends FormPostEncoder {
  // Make a Netty4 representation of the request.
  // Note: the request is *not* a FullHttpRequest to avoid the question of resource ownership
  //       with regard to the ByteBuf that is attached to a FullHttpRequest. See the inner
  //       comment for more details.
  private[this] def makeNetty4Request(config: RequestConfig): HttpRequest = {
    val version = Bijections.finagle.versionToNetty(config.version)
    val method = Bijections.finagle.methodToNetty(Method.Post)
    val uri = RequestConfig.resource(config)
    // We are deliberate in making a non-full request here because the Netty4 encoder may
    // wrap the underlying request, which can leak the FullRequests content ByteBuf. By
    // default it is Unpooled, but we might as well not bother with that.
    val request = new DefaultHttpRequest(version, method, uri)

    // Copy the headers to the Netty request.
    config.headers.foreach {
      case (field, values) =>
        values.foreach { v => request.headers.add(field, v) }
    }
    request
  }

  def encode(config: RequestConfig, multipart: Boolean): Request = {
    val dataFactory = new DefaultHttpDataFactory( /*useDisk*/ false) // we don't use disk

    val netty4Request = makeNetty4Request(config)
    // This request should *not* be a FullHttpRequest.
    if (netty4Request.isInstanceOf[FullHttpRequest]) {
      throw new IllegalStateException(
        s"Unexpected state: Expected the generated request to NOT" +
          s"be a full request: ${netty4Request.getClass.getSimpleName}"
      )
    }

    val encoder = new HttpPostRequestEncoder(dataFactory, netty4Request, multipart)
    try {
      config.formElements.foreach {
        case FileElement(name, content, contentType, filename, isText) =>
          addBodyFileUpload(
            encoder = encoder,
            factory = dataFactory,
            request = netty4Request,
            name = name,
            filename = filename.getOrElse(""),
            content = content,
            contentType = contentType.orNull,
            isText = isText
          )

        case SimpleElement(name, value) =>
          encoder.addBodyAttribute(name, value)
      }

      val encodedReq = encoder.finalizeRequest()

      // We're not going to send anything Transfer-Encoding: chunked, so strip any headers
      if (HttpUtil.isTransferEncodingChunked(encodedReq)) {
        val encodings = encodedReq.headers.getAll(Fields.TransferEncoding)
        if (encodings.contains("chunked")) {
          if (encodings.size == 1) {
            // only chunked, so we can just remove the header
            encodedReq.headers.remove(Fields.TransferEncoding)
          } else {
            val newList = new java.util.ArrayList[String](encodings.size - 1)
            val it = encodings.iterator()
            while (it.hasNext) it.next() match {
              case "chunked" => // nop
              case o => newList.add(o)
            }
            encodedReq.headers.set(Fields.TransferEncoding, newList)
          }
        }
      }

      encodedReq match {
        case fullReq: FullHttpRequest =>
          val req = Bijections.netty.fullRequestToFinagle(fullReq, new InetSocketAddress(0))
          req.contentLength match {
            case None => req.contentLength = req.content.length
            case Some(_) => // nop: already set
          }
          req

        case other =>
          val body = collectBodyFromEncoder(encoder)
          chunkedReqToFinagle(other, body)
      }
    } finally {
      encoder.close()
    }
  }

  private[this] def chunkedReqToFinagle(nettyReq: HttpRequest, body: Buf): Request = {
    val req = Bijections.netty.chunkedRequestToFinagle(
      nettyReq,
      Reader.value(Chunk.apply(body)),
      new InetSocketAddress(0)
    )

    req.setChunked(false)
    req.content = body
    req.contentLength = body.length
    req
  }

  private[this] def collectBodyFromEncoder(encoder: HttpPostRequestEncoder): Buf = {
    val allocator: ByteBufAllocator = UnpooledByteBufAllocator.DEFAULT
    // We don't need to worry about freeing direct buffers
    val acc = PooledByteBufAllocator.DEFAULT.compositeHeapBuffer(Int.MaxValue)

    try {
      while (!encoder.isEndOfInput) {
        val chunk = encoder.readChunk(allocator)
        acc.addComponent(true, chunk.content)
      }

      val arr = new Array[Byte](acc.readableBytes)
      acc.readBytes(arr)
      Buf.ByteArray.Owned(arr)
    } finally {
      acc.release()
    }
  }

  // allow specifying post body as ChannelBuffer, the logic is adapted from netty code.
  private[this] def addBodyFileUpload(
    encoder: HttpPostRequestEncoder,
    factory: HttpDataFactory,
    request: HttpRequest,
    name: String,
    filename: String,
    content: Buf,
    contentType: String,
    isText: Boolean
  ): Unit = {
    require(name != null)
    require(filename != null)
    require(content != null)

    val uploadContentType =
      if (contentType == null) {
        if (isText) MediaType.PlainText
        else MediaType.OctetStream
      } else {
        contentType
      }
    val contentTransferEncoding =
      if (!isText) BinaryTransferEncodingMechanism
      else null // netty is happy with the null

    val fileUpload = factory.createFileUpload(
      request,
      name,
      filename,
      uploadContentType,
      contentTransferEncoding,
      null, // charset
      content.length
    )
    fileUpload.setContent(ByteBufConversion.bufAsByteBuf(content))
    encoder.addBodyHttpData(fileUpload)
  }

  /** Fields defined in the Netty private class HttpPostBodyUtil. */
  private val BinaryTransferEncodingMechanism = "binary"
}
