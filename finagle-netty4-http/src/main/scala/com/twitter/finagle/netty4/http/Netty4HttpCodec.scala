package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.netty4.http.handler.UnpoolHttpHandler
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.handler.codec.http._
import java.net.InetSocketAddress
import io.netty.channel.embedded.EmbeddedChannel
import java.nio.charset.{StandardCharsets => Charsets}

/**
 * Utilities for encoding/decoding [[Request]]s and [[Response]]s to/from Strings
 * and byte arrays using Netty4 as the underlying implementation.
 */
private[finagle] object Netty4HttpCodec {

  /** Encode a [[Request]] to a String. */
  def encodeRequestToString(request: Request): String = {
    new String(encodeRequestToBytes(request), "UTF-8")
  }

  /** Encode a [[Request]] to a byte array */
  def encodeRequestToBytes(request: Request): Array[Byte] = {

    val ch = new EmbeddedChannel(
      new HttpRequestEncoder
    )

    ch.writeOneOutbound(Bijections.finagle.requestToNetty(request, request.contentLength))
    ch.flushOutbound()
    val acc = ch.alloc.compositeBuffer()

    try {
      while (!ch.outboundMessages.isEmpty) {
        acc.addComponent(true, ch.readOutbound[ByteBuf]())
      }

      val out = new Array[Byte](acc.readableBytes)
      acc.readBytes(out)
      out
    } finally {
      acc.release()
      ch.finishAndReleaseAll()
    }
  }

  /** Decode a [[Request]] from a String */
  def decodeStringToRequest(s: String): Request = {
    decodeBytesToRequest(s.getBytes(Charsets.UTF_8))
  }

  /** Decode a [[Request]] from a byte array */
  def decodeBytesToRequest(b: Array[Byte]): Request = {
    val ch = new EmbeddedChannel(
      new HttpRequestDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue),
      new HttpObjectAggregator(Int.MaxValue),
      UnpoolHttpHandler // Make sure nothing is ref-counted
    )

    try {
      ch.writeInbound(Unpooled.wrappedBuffer(b))
      ch.flushInbound()

      // Should be exactly one message or the input data was likely malformed
      // Note: we perform the assert before reading so that the if it fails any
      // messages are still freed in the finally block
      assert(ch.inboundMessages.size == 1)
      val nettyReq = ch.readInbound[FullHttpRequest]()

      Bijections.netty.fullRequestToFinagle(nettyReq, new InetSocketAddress(0))
    } finally {
      ch.finishAndReleaseAll()
    }
  }

  /** Encode a [[Response]] to a String */
  def encodeResponseToString(response: Response): String = {
    val ch = new EmbeddedChannel(
      new HttpResponseEncoder
    )

    val acc = ch.alloc().compositeBuffer()

    try {
      ch.writeOutbound(Bijections.finagle.fullResponseToNetty(response))
      ch.flushOutbound()

      while (!ch.outboundMessages.isEmpty) {
        acc.addComponent(true, ch.readOutbound[ByteBuf]())
      }

      acc.toString(Charsets.UTF_8)
    } finally {
      acc.release()
      ch.finishAndReleaseAll()
    }
  }

  /** Decode a [[Response]] from a String */
  def decodeStringToResponse(s: String): Response = {
    decodeBytesToResponse(s.getBytes(Charsets.UTF_8))
  }

  /** Decode a [[Response]] from a byte array */
  def decodeBytesToResponse(b: Array[Byte]): Response = {
    val ch = new EmbeddedChannel(
      new HttpResponseDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue),
      new HttpObjectAggregator(Int.MaxValue),
      UnpoolHttpHandler // Make sure nothing is ref-counted
    )

    try {
      ch.writeInbound(Unpooled.wrappedBuffer(b))
      ch.flushInbound()

      // Should be exactly one message or the message was likely malformed
      // Note: we perform the assert before reading so that the if it fails any
      // messages are still freed in the finally block
      assert(ch.inboundMessages.size == 1)
      val resp = ch.readInbound[FullHttpResponse]()

      Bijections.netty.fullResponseToFinagle(resp)
    } finally {
      ch.finishAndReleaseAll()
    }
  }
}
