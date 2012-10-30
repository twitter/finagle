package com.twitter.finagle.postgres.protocol

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.logging.Logger

trait FrontendMessage {
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

case class PasswordMessage(password: String) extends FrontendMessage {
  private val logger = Logger(getClass.getName)

  def asPacket() = PacketBuilder('p')
    .writeCString(password)
    .toPacket
}

case class Query(str: String) extends FrontendMessage {
  private val logger = Logger(getClass.getName)

  def asPacket() = PacketBuilder('Q')
    .writeCString(str)
    .toPacket
}
