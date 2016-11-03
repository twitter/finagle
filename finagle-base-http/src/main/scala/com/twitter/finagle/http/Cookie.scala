package com.twitter.finagle.http

import com.twitter.conversions.time._
import com.twitter.util.Duration
import org.jboss.netty.handler.codec.http.{Cookie => NettyCookie, DefaultCookie}
import scala.collection.JavaConverters._

/** Scala wrapper around Netty cookies. */
class Cookie(private[http] val underlying: NettyCookie) {
  def this(name: String, value: String) = {
    this(new DefaultCookie(name, value))
  }

  def comment: String    = underlying.getComment
  def commentUrl: String = underlying.getCommentUrl
  def domain: String     = underlying.getDomain
  def maxAge: Duration   = underlying.getMaxAge.seconds
  def name: String       = underlying.getName
  def path: String       = underlying.getPath
  def ports: Set[Int]    = underlying.getPorts.asScala.toSet map { i: Integer => i.intValue }
  def value: String      = underlying.getValue
  def version: Int       = underlying.getVersion
  def httpOnly: Boolean  = underlying.isHttpOnly
  def isDiscard: Boolean = underlying.isDiscard
  def isSecure: Boolean  = underlying.isSecure

  def comment_=(comment: String)       { underlying.setComment(comment) }
  def commentUrl_=(commentUrl: String) { underlying.setCommentUrl(commentUrl) }
  def domain_=(domain: String)         { underlying.setDomain(domain) }
  def maxAge_=(maxAge: Duration)       { underlying.setMaxAge(maxAge.inSeconds) }
  def path_=(path: String)             { underlying.setPath(path) }
  def ports_=(ports: Seq[Int])         { underlying.setPorts(ports: _*) }
  def value_=(value: String)           { underlying.setValue(value) }
  def version_=(version: Int)          { underlying.setVersion(version) }
  def httpOnly_=(httpOnly: Boolean)    { underlying.setHttpOnly(httpOnly) }
  def isDiscard_=(discard: Boolean)    { underlying.setDiscard(discard) }
  def isSecure_=(secure: Boolean)      { underlying.setSecure(secure) }

  override def equals(obj: Any): Boolean = obj match {
    case c: Cookie => underlying.equals(c.underlying)
    case _ => false
  }

  override def hashCode(): Int = underlying.hashCode()
}
