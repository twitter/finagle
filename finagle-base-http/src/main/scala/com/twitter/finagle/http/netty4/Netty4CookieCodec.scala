package com.twitter.finagle.http.netty4

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.{Cookie, CookieCodec, CookieMap}
import com.twitter.finagle.http.cookie.SameSiteCodec
import io.netty.handler.codec.http.cookie.{
  ClientCookieDecoder => NettyClientCookieDecoder,
  ClientCookieEncoder => NettyClientCookieEncoder,
  Cookie => NettyCookie,
  DefaultCookie => NettyDefaultCookie,
  ServerCookieDecoder => NettyServerCookieDecoder,
  ServerCookieEncoder => NettyServerCookieEncoder
}
import java.util.{BitSet => JBitSet}
import scala.collection.JavaConverters._

private[finagle] object Netty4CookieCodec extends CookieCodec {
  // not stateful, so safe to re-use
  private[this] val clientEncoder = NettyClientCookieEncoder.STRICT
  private[this] val serverEncoder = NettyServerCookieEncoder.STRICT
  private[this] val clientDecoder = NettyClientCookieDecoder.STRICT
  private[this] val serverDecoder = NettyServerCookieDecoder.STRICT

  // These are the chars that trigger double-quote-wrapping of values in Netty 3, minus the
  // characters that are prohibited in Netty 4.
  private[this] val ShouldWrapCharsBitSet: JBitSet = {
    val bs = new JBitSet
    "()/:<?@[]=>{}".foreach(bs.set(_))
    bs
  }

  def encodeClient(cookies: Iterable[Cookie]): String =
    // N4 Encoder returns null if cookies is empty
    if (cookies.isEmpty) ""
    else clientEncoder.encode(cookies.map(cookieToNetty).asJava)

  def encodeServer(cookie: Cookie): String = {
    val encoded = serverEncoder.encode(cookieToNetty(cookie))
    if (CookieMap.includeSameSite) SameSiteCodec.encodeSameSite(cookie, encoded)
    else encoded
  }

  def decodeClient(header: String): Option[Iterable[Cookie]] = {
    val cookie = clientDecoder.decode(header)
    if (cookie != null) {
      val decoded = cookieToFinagle(cookie)
      val finagleCookie =
        if (CookieMap.includeSameSite) SameSiteCodec.decodeSameSite(header, decoded)
        else decoded
      Some(Seq(finagleCookie))
    } else None
  }

  def decodeServer(header: String): Option[Iterable[Cookie]] = {
    val cookies = serverDecoder.decodeAll(header).asScala.map(cookieToFinagle)
    if (!cookies.isEmpty) Some(cookies)
    else None
  }

  private[this] def shouldWrap(cookie: Cookie): Boolean =
    Cookie.stringContains(cookie.value, ShouldWrapCharsBitSet)

  private[netty4] val cookieToNetty: Cookie => NettyCookie = c => {
    val nc = new NettyDefaultCookie(c.name, c.value)
    nc.setDomain(c.domain)
    nc.setPath(c.path)
    // We convert the Durations to Ints to circumvent maxAge being
    // Int.MinValue.seconds, which does not equal Duration.Bottom, even though
    // they have the same integer value in seconds.
    if (c.maxAge.inSeconds != Cookie.DefaultMaxAge.inSeconds) {
      nc.setMaxAge(c.maxAge.inSeconds)
    }
    nc.setSecure(c.secure)
    nc.setHttpOnly(c.httpOnly)
    if (shouldWrap(c)) nc.setWrap(true)
    nc
  }

  private[netty4] val cookieToFinagle: NettyCookie => Cookie = nc => {
    val cookie = new Cookie(
      name = nc.name,
      value = nc.value,
      domain = Option(nc.domain()),
      path = Option(nc.path()),
      secure = nc.isSecure(),
      httpOnly = nc.isHttpOnly()
    )

    // Note: Long.MinValue is what Netty 4 uses to indicate "never expires."
    if (nc.maxAge() != Long.MinValue) cookie.maxAge(Some(nc.maxAge().seconds))
    else cookie
  }
}
