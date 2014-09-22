package com.twitter.finagle.httpx.netty

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http.{HttpHeaders, HttpMessage, HttpVersion}
import java.lang.{Iterable => JIterable}
import java.util.{List => JList, Map => JMap, Set => JSet}


/** Proxy for HttpMessage.  Used by Request and Response. */
private[finagle] trait HttpMessageProxy extends Proxy {
  protected[finagle] def httpMessage: HttpMessage
  protected[finagle] def getHttpMessage(): HttpMessage = httpMessage
  def self = httpMessage

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def getHeader(name: String): String =
    httpMessage.getHeader(name)

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def getHeaders(name: String): JList[String] =
    httpMessage.getHeaders(name)

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def getHeaders(): JList[JMap.Entry[String, String]] =
    httpMessage.getHeaders()

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def containsHeader(name: String): Boolean =
    httpMessage.containsHeader(name)

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def getHeaderNames(): JSet[String] =
    httpMessage.getHeaderNames()

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def addHeader(name: String, value: Object): Unit =
    httpMessage.addHeader(name, value)

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def setHeader(name: String, value: Object): Unit =
    httpMessage.setHeader(name, value)

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def setHeader(name: String, values: JIterable[_]): Unit =
    httpMessage.setHeader(name, values)

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def removeHeader(name: String): Unit =
    httpMessage.removeHeader(name)

  @deprecated("deprecated in netty", "6.8.0")
  protected[finagle] def clearHeaders(): Unit =
    httpMessage.clearHeaders()

  protected[finagle] def getProtocolVersion(): HttpVersion =
    httpMessage.getProtocolVersion()

  protected[finagle] def setProtocolVersion(version: HttpVersion): Unit =
    httpMessage.setProtocolVersion(version)

  protected[finagle] def headers(): HttpHeaders =
    httpMessage.headers()

  protected[finagle] def getContent(): ChannelBuffer =
    httpMessage.getContent()

  protected[finagle] def setContent(content: ChannelBuffer): Unit =
    httpMessage.setContent(content)

  def isChunked: Boolean = httpMessage.isChunked()

  def setChunked(chunked: Boolean): Unit =
    httpMessage.setChunked(chunked)
}
