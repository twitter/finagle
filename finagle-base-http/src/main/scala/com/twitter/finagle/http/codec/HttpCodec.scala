package com.twitter.finagle.http.codec

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.http.netty3.Bijections
import com.twitter.finagle.http.netty3.Bijections.responseFromNetty
import java.nio.charset.{StandardCharsets => Charsets}
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.embedder.{DecoderEmbedder, EncoderEmbedder}
import org.jboss.netty.handler.codec.http.{
  HttpRequest,
  HttpRequestDecoder,
  HttpRequestEncoder,
  HttpResponse,
  HttpResponseDecoder,
  HttpResponseEncoder
}

/**
 * Testing utilities for encoding/decoding [[Request]]s and [[Response]]s to/from Strings
 * and byte arrays.
 */
object HttpCodec {

  /** Encode a [[Request]] to a String. */
  def encodeRequestToString(request: Request): String = {
    new String(encodeRequestToBytes(request), "UTF-8")
  }

  /** Encode a [[Request]] tp a byte array */
  def encodeRequestToBytes(request: Request): Array[Byte] = {
    val encoder = new EncoderEmbedder[ChannelBuffer](new HttpRequestEncoder)
    encoder.offer(Bijections.requestToNetty(request))
    val buffer = encoder.poll()
    val bytes = new Array[Byte](buffer.readableBytes())
    buffer.readBytes(bytes)
    bytes
  }

  /** Decode a [[Request]] from a String */
  def decodeStringToRequest(s: String): Request = {
    decodeBytesToRequest(s.getBytes(Charsets.UTF_8))
  }

  /** Decode a [[Request]] from a byte array */
  def decodeBytesToRequest(b: Array[Byte]): Request = {
    val decoder = new DecoderEmbedder(
      new HttpRequestDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue)
    )
    decoder.offer(ChannelBuffers.wrappedBuffer(b))
    val req = decoder.poll().asInstanceOf[HttpRequest]
    assert(req ne null)

    Bijections.requestFromNetty(req)
  }

  /** Encode a [[Response]] to a String */
  def encodeResponseToString(response: Response): String = {
    val encoder = new EncoderEmbedder[ChannelBuffer](new HttpResponseEncoder)
    encoder.offer(Bijections.responseToNetty(response))
    val buffer = encoder.poll()
    buffer.toString(Charsets.UTF_8)
  }

  /** Decode a [[Response]] from a String */
  def decodeStringToResponse(s: String): Response = {
    decodeBytesToResponse(s.getBytes(Charsets.UTF_8))
  }

  /** Decode a [[Response]] from a byte array */
  def decodeBytesToResponse(b: Array[Byte]): Response = {
    val decoder = new DecoderEmbedder(
      new HttpResponseDecoder(Int.MaxValue, Int.MaxValue, Int.MaxValue)
    )
    decoder.offer(ChannelBuffers.wrappedBuffer(b))
    val res = decoder.poll().asInstanceOf[HttpResponse]
    assert(res ne null)
    responseFromNetty(res)
  }
}
