package com.twitter.finagle.http.codec

import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.netty4.http.Netty4HttpCodec

/**
 * Utilities for encoding/decoding [[Request]]s and [[Response]]s to/from Strings
 * and byte arrays.
 */
object HttpCodec {

  /** Encode a [[Request]] to a String. */
  def encodeRequestToString(request: Request): String =
    Netty4HttpCodec.encodeRequestToString(request)

  /** Encode a [[Request]] to a byte array */
  def encodeRequestToBytes(request: Request): Array[Byte] =
    Netty4HttpCodec.encodeRequestToBytes(request)

  /** Decode a [[Request]] from a String */
  def decodeStringToRequest(s: String): Request =
    Netty4HttpCodec.decodeStringToRequest(s)

  /** Decode a [[Request]] from a byte array */
  def decodeBytesToRequest(b: Array[Byte]): Request =
    Netty4HttpCodec.decodeBytesToRequest(b)

  /** Encode a [[Response]] to a String */
  def encodeResponseToString(response: Response): String =
    Netty4HttpCodec.encodeResponseToString(response)

  /** Decode a [[Response]] from a String */
  def decodeStringToResponse(s: String): Response =
    Netty4HttpCodec.decodeStringToResponse(s)

  /** Decode a [[Response]] from a byte array */
  def decodeBytesToResponse(b: Array[Byte]): Response =
    Netty4HttpCodec.decodeBytesToResponse(b)
}
