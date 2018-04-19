package com.twitter.finagle.http.netty3

import com.twitter.finagle.http._
import com.twitter.finagle.http.cookie.SameSiteCodec
import com.twitter.finagle.http.netty3.Bijections.{cookieFromNettyInjection, cookieToNettyInjection}
import org.jboss.netty.handler.codec.http.{
  CookieDecoder => NettyCookieDecoder,
  CookieEncoder => NettyCookieEncoder
}
import org.jboss.netty.handler.codec.http.{Cookie => Netty3Cookie}
import scala.collection.JavaConverters._

/**
 * A [[CookieCodec]] that uses the Netty 3 versions of encoders/decoders as the underlying
 * encoders/decoders.
 */
private[http] object Netty3CookieCodec extends CookieCodec {

  // not stateful, so safe to re-use
  private[this] val sameSiteDecoder = new SameSiteSupportingCookieDecoder
  private[this] val netty3Decoder = new NettyCookieDecoder

  def encodeClient(cookies: Iterable[Cookie]): String = {
    val encoder = new NettyCookieEncoder(false /* encode client-style cookies */)
    cookies.foreach { cookie =>
      encoder.addCookie(Bijections.from[Cookie, Netty3Cookie](cookie))
    }
    encoder.encode()
  }

  def encodeServer(cookie: Cookie): String = {
    val encoder = new NettyCookieEncoder(true /* encode server-style cookies */)
    encoder.addCookie(Bijections.from[Cookie, Netty3Cookie](cookie))
    val encoded = encoder.encode()
    if (CookieMap.includeSameSite) SameSiteCodec.encodeSameSite(cookie, encoded)
    else encoded
  }

  def decodeClient(header: String): Option[Iterable[Cookie]] = {
    try {
      val cookies =
        if (CookieMap.includeSameSite) sameSiteDecoder.decode(header).asScala
        else netty3Decoder.decode(header).asScala

      Some(cookies.map { cookie: Netty3Cookie =>
        Bijections.from[Netty3Cookie, Cookie](cookie)
      })
    } catch {
      case e: IllegalArgumentException => None
    }
  }

  def decodeServer(header: String): Option[Iterable[Cookie]] =
    try {
      val cookies =
        if (CookieMap.includeSameSite) sameSiteDecoder.decode(header).asScala
        else netty3Decoder.decode(header).asScala

      Some(cookies.map { cookie: Netty3Cookie =>
        Bijections.from[Netty3Cookie, Cookie](cookie)
      })
    } catch {
      case e: IllegalArgumentException => None
    }

}
