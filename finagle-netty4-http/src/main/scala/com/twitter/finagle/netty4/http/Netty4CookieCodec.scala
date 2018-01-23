package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.{Cookie, CookieCodec}
import io.netty.handler.codec.http.cookie.{
  ClientCookieDecoder => NettyClientCookieDecoder,
  ClientCookieEncoder => NettyClientCookieEncoder,
  ServerCookieDecoder => NettyServerCookieDecoder,
  ServerCookieEncoder => NettyServerCookieEncoder
}
import scala.collection.JavaConverters._

private[http] object Netty4CookieCodec extends CookieCodec {
  // not stateful, so safe to re-use
  private[this] val clientEncoder = NettyClientCookieEncoder.STRICT
  private[this] val serverEncoder = NettyServerCookieEncoder.STRICT
  private[this] val clientDecoder = NettyClientCookieDecoder.STRICT
  private[this] val serverDecoder = NettyServerCookieDecoder.STRICT

  def encodeClient(cookies: Iterable[Cookie]): String =
    // N4 Encoder returns null if cookies is empty
    if (cookies.isEmpty) ""
    else clientEncoder.encode(cookies.map(Bijections.finagle.cookieToNetty).asJava)

  def encodeServer(cookie: Cookie): String =
    serverEncoder.encode(Bijections.finagle.cookieToNetty(cookie))

  def decodeClient(header: String): Option[Iterable[Cookie]] = {
    val cookie = clientDecoder.decode(header)
    if (cookie != null) Some(Seq(Bijections.netty.cookieToFinagle(cookie)))
    else None
  }

  def decodeServer(header: String): Option[Iterable[Cookie]] = {
    val cookies = serverDecoder.decode(header).asScala.map(Bijections.netty.cookieToFinagle)
    if (!cookies.isEmpty) Some(cookies)
    else None
  }
}
