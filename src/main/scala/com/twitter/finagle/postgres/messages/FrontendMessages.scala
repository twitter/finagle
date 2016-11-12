package com.twitter.finagle.postgres.messages

import com.twitter.finagle.postgres.values.{Strings, Convert}

import org.jboss.netty.buffer.ChannelBuffer

/**
 * Messages sent to Postgres from the client.
 */
trait FrontendMessage extends Message {
  def asPacket(): Packet
}

case class StartupMessage(user: String, database: String) extends FrontendMessage {
  def asPacket() = PacketBuilder()
    .writeShort(3)
    .writeShort(0)
    .writeCString("user")
    .writeCString(user)
    .writeCString("database")
    .writeCString(database)
    .writeByte(0)
    .toPacket
}

case class SslRequestMessage() extends FrontendMessage {
  def asPacket() = PacketBuilder()
    .writeShort(1234)
    .writeShort(5679)
    .toPacket
}

case class PasswordMessage(password: String) extends FrontendMessage {
  def asPacket() = PacketBuilder('p')
    .writeCString(password)
    .toPacket
}

case class Query(str: String) extends FrontendMessage {
  def asPacket() = PacketBuilder('Q')
    .writeCString(str)
    .toPacket
}

case class Parse(
      name: String = Strings.empty, query: String = "", paramTypes: Seq[Int] = Seq()) extends FrontendMessage {
  def asPacket() = {
    val builder = PacketBuilder('P')
      .writeCString(name)
      .writeCString(query)
      .writeShort(Convert.asShort(paramTypes.length))

    for (param <- paramTypes) {
      builder.writeInt(param)
    }
    builder.toPacket
  }
}

case class Bind(portal: String = Strings.empty, name: String = Strings.empty, formats: Seq[Int] = Seq(),
                params: Seq[ChannelBuffer] = Seq(), resultFormats: Seq[Int] = Seq()) extends FrontendMessage {
  def asPacket() = {
    val builder = PacketBuilder('B')
      .writeCString(portal)
      .writeCString(name)

    if (formats.isEmpty) {
      builder.writeShort(0)
    } else {
      builder.writeShort(formats.length.toShort)
      for (format <- formats) {
        builder.writeShort(format.toShort)
      }
    }

    builder.writeShort(Convert.asShort(params.length))

    for (param <- params) {
      param.resetReaderIndex()
      builder.writeBuf(param)
    }

    if (resultFormats.isEmpty) {
      builder.writeShort(0)
    } else {
      builder.writeShort(resultFormats.length.toShort)
      for (format <- resultFormats) {
        builder.writeShort(format.toShort)
      }
    }

    builder.toPacket
  }
}

case class Execute(name: String = Strings.empty, maxRows: Int = 0) extends FrontendMessage {
  def asPacket() = {
    PacketBuilder('E')
      .writeCString(name)
      .writeInt(maxRows)
      .toPacket
  }
}

case class Describe(portal: Boolean = true, name: String = new String) extends FrontendMessage {
  def asPacket() = {
    PacketBuilder('D')
      .writeChar(if (portal) 'P' else 'S')
      .writeCString(name)
      .toPacket
  }
}

object Flush extends FrontendMessage {
  def asPacket() = {
    PacketBuilder('H')
      .toPacket
  }
}

object Sync extends FrontendMessage {
  def asPacket() = {
    PacketBuilder('S')
      .toPacket
  }
}

object Terminate extends FrontendMessage {
  def asPacket() = PacketBuilder('X').toPacket
}
