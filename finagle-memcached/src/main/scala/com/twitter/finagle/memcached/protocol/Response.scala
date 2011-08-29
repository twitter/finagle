package com.twitter.finagle.memcached.protocol

import org.jboss.netty.buffer.ChannelBuffer

sealed abstract class Response
case class NotFound()                     extends Response
case class Stored()                       extends Response
case class NotStored()                    extends Response
case class Exists()                       extends Response
case class Deleted()                      extends Response
case class Error(cause: Exception)        extends Response

case class Values(values: Seq[Value]) extends Response
case class Number(value: Long)         extends Response

case class Value(key: ChannelBuffer, value: ChannelBuffer, casUnique: Option[ChannelBuffer] = None)
