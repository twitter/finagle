package com.twitter.finagle.http.netty4

import com.twitter.conversions.time._
import com.twitter.finagle.http.{Cookie, CookieCodec}
import io.netty.handler.codec.http.cookie.{
  Cookie => NettyCookie,
  ClientCookieDecoder => NettyClientCookieDecoder,
  ClientCookieEncoder => NettyClientCookieEncoder,
  DefaultCookie => NettyDefaultCookie,
  ServerCookieDecoder => NettyServerCookieDecoder,
  ServerCookieEncoder => NettyServerCookieEncoder
}
import scala.collection.JavaConverters._

private[finagle] object Netty4CookieCodec extends CookieCodec {
  // not stateful, so safe to re-use
  private[this] val clientEncoder = NettyClientCookieEncoder.STRICT
  private[this] val serverEncoder = NettyServerCookieEncoder.STRICT
  private[this] val clientDecoder = NettyClientCookieDecoder.STRICT
  private[this] val serverDecoder = NettyServerCookieDecoder.STRICT

  def encodeClient(cookies: Iterable[Cookie]): String =
    // N4 Encoder returns null if cookies is empty
    if (cookies.isEmpty) ""
    else clientEncoder.encode(cookies.map(cookieToNetty).asJava)

  def encodeServer(cookie: Cookie): String =
    serverEncoder.encode(cookieToNetty(cookie))

  def decodeClient(header: String): Option[Iterable[Cookie]] = {
    val cookie = clientDecoder.decode(header)
    if (cookie != null) Some(Seq(cookieToFinagle(cookie)))
    else None
  }

  def decodeServer(header: String): Option[Iterable[Cookie]] = {
    val cookies = serverDecoder.decode(header).asScala.map(cookieToFinagle)
    if (!cookies.isEmpty) Some(cookies)
    else None
  }

  private[netty4] val cookieToNetty: Cookie => NettyCookie = c => {
    val nc = new NettyDefaultCookie(c.name, c.value)
    nc.setDomain(c.domain)
    nc.setPath(c.path)
    if (c.maxAge != Cookie.DefaultMaxAge) {
      nc.setMaxAge(c.maxAge.inSeconds)
    }
    nc.setSecure(c.secure)
    nc.setHttpOnly(c.httpOnly)
    nc
  }

  private[netty4] val cookieToFinagle: NettyCookie => Cookie = nc => {
    val cookie = new Cookie(
      name = nc.name,
      value = nc.value,
      domain = Option(nc.domain()),
      path = Option(nc.path()),
      secure = nc.isSecure(),
      httpOnly = nc.isHttpOnly())

    if (nc.maxAge() != Long.MinValue) cookie.maxAge(Some(nc.maxAge().seconds))
    else cookie
  }
}
