package com.twitter.finagle.kestrel.protocol

import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.util.Time

object Show {
  private[this] val DELIMETER     = "\r\n"   .getBytes
  private[this] val VALUE         = "VALUE"  .getBytes
  private[this] val ZERO          = "0"      .getBytes
  private[this] val SPACE         = " "      .getBytes
  private[this] val GET           = "get"    .getBytes
  private[this] val SET           = "set"    .getBytes
  private[this] val END           = "END"    .getBytes

  private[this] val STORED        = copiedBuffer("STORED".getBytes,       DELIMETER)
  private[this] val NOT_FOUND     = copiedBuffer("NOT_FOUND".getBytes,    DELIMETER)
  private[this] val DELETED       = copiedBuffer("DELETED".getBytes,      DELIMETER)

  private[this] val ERROR         = copiedBuffer("ERROR".getBytes,        DELIMETER)
  private[this] val CLIENT_ERROR  = copiedBuffer("CLIENT_ERROR".getBytes, DELIMETER)
  private[this] val SERVER_ERROR  = copiedBuffer("SERVER_ERROR".getBytes, DELIMETER)

  def apply(response: Response) = {
    response match {
      case Stored()       => STORED
      case Deleted()      => DELETED
      case NotFound()     => NOT_FOUND
      case Values(values) =>
        val buffer = ChannelBuffers.dynamicBuffer(100 * values.size)
        val shown = values map { case Value(key, value) =>
          buffer.writeBytes(VALUE)
          buffer.writeBytes(SPACE)
          buffer.writeBytes(key)
          buffer.writeBytes(SPACE)
          buffer.writeBytes(ZERO)
          buffer.writeBytes(SPACE)
          buffer.writeBytes(value.readableBytes.toString.getBytes)
          buffer.writeBytes(DELIMETER)
          value.resetReaderIndex()
          buffer.writeBytes(value)
          buffer.writeBytes(DELIMETER)
        }
        buffer.writeBytes(END)
        buffer.writeBytes(DELIMETER)
        buffer
    }
  }

  def apply(command: Command): ChannelBuffer = {
    command match {
      case Set(key, expiry, value) =>
        showStorageCommand(SET, key, 0, expiry, value)
      case Get(key, options) =>
        val buffer = ChannelBuffers.dynamicBuffer(50)
        buffer.writeBytes(GET)
        buffer.writeBytes(SPACE)
        buffer.writeBytes(key)
        buffer.writeBytes(SPACE)
        buffer.writeBytes(DELIMETER)
        buffer
      case Delete(key) =>
        val buffer = ChannelBuffers.dynamicBuffer(30)
        buffer.writeBytes(SPACE)
        buffer.writeBytes(key)
        buffer.writeBytes(DELIMETER)
        buffer
    }
  }

  def apply(throwable: Throwable) = throwable match {
    case e: NonexistentCommand => ERROR
    case e: ClientError        => CLIENT_ERROR
    case e: ServerError        => SERVER_ERROR
    case _                     => throw throwable
  }

  @inline private[this] def showStorageCommand(
    name: Array[Byte], key: ChannelBuffer, flags: Int, expiry: Time, value: ChannelBuffer) = {
    val buffer = ChannelBuffers.dynamicBuffer(50 + value.readableBytes)
    buffer.writeBytes(name)
    buffer.writeBytes(SPACE)
    buffer.writeBytes(key)
    buffer.writeBytes(SPACE)
    buffer.writeBytes(flags.toString.getBytes)
    buffer.writeBytes(SPACE)
    buffer.writeBytes(expiry.inSeconds.toString.getBytes)
    buffer.writeBytes(SPACE)
    buffer.writeBytes(value.readableBytes.toString.getBytes)
    buffer.writeBytes(DELIMETER)
    buffer.writeBytes(value)
    buffer.writeBytes(DELIMETER)
    buffer
  }
}