package com.twitter.finagle.postgres.protocol

import scala.collection.mutable.MutableList

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

import com.twitter.logging.Logger

case class Packet(code: Char, length: Int, content: ChannelBuffer)

trait FrontendMessage {
  def encode(): ChannelBuffer // TODO encode to a packet
}

case class StartupMessage(user: String, database: String) extends FrontendMessage {
  def encode(): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer()
    buffer.writeInt(0)
    buffer.writeShort(3)
    buffer.writeShort(0)

    Buffers.writeCString(buffer, "user")
    Buffers.writeCString(buffer, user)

    Buffers.writeCString(buffer, "database")
    Buffers.writeCString(buffer, database)

    buffer.writeByte(0)

    val index = buffer.writerIndex()

    buffer.markWriterIndex()
    buffer.writerIndex(0)
    buffer.writeInt(index)
    buffer.resetWriterIndex()

    buffer
  }
}

case class PasswordMessage(password: String) extends FrontendMessage {
  def encode(): ChannelBuffer = {
    val buffer: ChannelBuffer = ChannelBuffers.dynamicBuffer()

    buffer.writeByte('Q')
    buffer.writeByte(0)

    Buffers.writeCString(buffer, password)
    buffer.writeByte(0)

    val index = buffer.writerIndex()
    buffer.markWriterIndex()
    buffer.writerIndex(1)
    buffer.writeInt(index)
    buffer.resetWriterIndex()

    buffer
  }
}

case class Query(str: String) extends FrontendMessage {
  private val logger = Logger(getClass.getName)

  def encode(): ChannelBuffer = {
    val buffer: ChannelBuffer = ChannelBuffers.dynamicBuffer()

    buffer.writeByte('Q')
    buffer.writeInt(0)
    Buffers.writeCString(buffer, str)

    val index = buffer.writerIndex() - 1
    buffer.markWriterIndex()
    buffer.writerIndex(1)
    buffer.writeInt(index)
    buffer.resetWriterIndex()

    logger.debug("buffer " + buffer)

    buffer
  }
}

/**
 * Response sent from Postgres back to the client.
 */
trait BackendMessage {
}

case class ErrorResponse(msg: Option[String]) extends BackendMessage {
}

case class AuthenticationOk() extends BackendMessage {

}

case class AuthenticationMD5Password(salt: Array[Byte]) extends BackendMessage {

}

case class AuthenticationCleartextPassword() extends BackendMessage {

}

case class ParameterStatus(name: String, value: String) extends BackendMessage {

}

case class BackendKeyData(processId: Int, secretKey: Int) extends BackendMessage {

}

case class RowDescription(fields: IndexedSeq[FieldDescription]) extends BackendMessage {

}

case class FieldDescription(
  name: String,
  tableId: Int,
  columnNumber: Int,
  dataType: Int,
  dataTypeSize: Int,
  dataTypeMondifier: Int,
  fieldFormat: Int) {
}

case class DataRow(data: IndexedSeq[ChannelBuffer]) extends BackendMessage {

}

// TODO parse affected rows from a string
case class CommandComplete(tag: String) extends BackendMessage {}

// TODO maybe use some enum
case class ReadyForQuery(status: Char) extends BackendMessage {

}

class BackendMessageParser {

  private val logger = Logger(getClass.getName)

  def parse(packet: Packet): Option[BackendMessage] = {
    val result: Option[BackendMessage] = packet.code match {
      case 'R' =>
        parseR(packet)
      case 'E' =>
        parseE(packet)
      case 'K' =>
        parseK(packet)
      case 'd' =>
        parseSmallD(packet)
      case 'D' =>
        parseD(packet)
      case '3' =>
        logger.error("'3' Not implemented yet")
        throw new UnsupportedOperationException("'3' Not implemented yet")
      case 'C' =>
        parseC(packet)
      case 'G' =>
        logger.error("'G' Not implemented yet")
        throw new UnsupportedOperationException("'G' Not implemented yet")
      case 'H' =>
        logger.error("'H' Not implemented yet")
        throw new UnsupportedOperationException("'H' Not implemented yet")
      case 'W' =>
        logger.error("'W' Not implemented yet")

        throw new UnsupportedOperationException("'W' Not implemented yet")
      case 'I' =>
        logger.error("'I' Not implemented yet")
        throw new UnsupportedOperationException("'I'Not implemented yet")
      case 'V' =>
        logger.error("'V' Not implemented yet")
        throw new UnsupportedOperationException("'V' Not implemented yet")
      case 'n' =>
        logger.error("'n' Not implemented yet")

        throw new UnsupportedOperationException("'n' Not implemented yet")
      case 'N' =>
        logger.error("'N' Not implemented yet")

        throw new UnsupportedOperationException("'N' Not implemented yet")
      case 'A' =>
        logger.error("'A' Not implemented yet")

        throw new UnsupportedOperationException("'A' Not implemented yet")
      case 't' =>
        logger.error("'T' Not implemented yet")

        throw new UnsupportedOperationException("'T' Not implemented yet")
      case 'S' =>
        parseS(packet)
      case '1' =>
        logger.error("'1' Not implemented yet")

        throw new UnsupportedOperationException("'1' Not implemented yet")
      case 's' =>
        logger.error("'s' Not implemented yet")

        throw new UnsupportedOperationException("'s' Not implemented yet")
      case 'Z' =>
        parseZ(packet)
      case 'T' =>
        parseT(packet)

      case unknown =>
        None
    }

    logger.debug("Result " + result)
    result
  }

  def parseR(packet: Packet): Option[BackendMessage] = {
    logger.debug("parsing R")
    logger.debug("Packet " + packet)

    packet.length match {
      case 8 =>
        val content = packet.content
        val code = content.readInt()

        code match {
          case 3 =>
            Some(new AuthenticationCleartextPassword())
          case 0 =>
            Some(new AuthenticationOk())
          case _ =>
            None
        }

      case 12 =>
        val salt = Array[Byte](4)
        packet.content.readBytes(salt)
        Some(new AuthenticationMD5Password(salt))

      case _ =>
        None
    }

  }

  def parseE(packet: Packet): Option[BackendMessage] = {
    logger.debug("parsing E")
    logger.debug("Packet " + packet)

    val builder = new StringBuilder()
    while (packet.content.readable) {
      {
        builder.append(Buffers.readCString(packet.content))
      }
    }

    Some(new ErrorResponse(Some(builder.toString)))
  }

  def parseZ(packet: Packet): Option[BackendMessage] = {
    logger.debug("parsing Z")
    logger.debug("Packet " + packet)

    if (packet.length != 5) {
      throw new IllegalStateException("Unexpected message length ")
    }

    val status: Char = packet.content.readByte().asInstanceOf[Char]

    Some(new ReadyForQuery(status))

  }

  def parseK(packet: Packet): Option[BackendMessage] = {
    logger.debug("parsing K")
    logger.debug("Packet " + packet)

    if (packet.length != 12) {
      throw new IllegalStateException("Unexpected message length ")
    }

    val processId = packet.content.readInt
    val secretKey = packet.content.readInt

    Some(new BackendKeyData(processId, secretKey))
  }

  def parseSmallD(packet: Packet): Option[BackendMessage] = {
    throw new UnsupportedOperationException("'d' Not implemented yet")
  }

  def parseD(packet: Packet): Option[BackendMessage] = {

    logger.debug("parsing D")
    logger.debug("Packet " + packet)

    val fieldNumber = packet.content.readShort
    logger.debug("fields " + fieldNumber)
    val fields = new Array[ChannelBuffer](fieldNumber)

    0.until(fieldNumber).foreach { i =>
      val length = packet.content.readInt
      val index = packet.content.readerIndex
      val slice = packet.content.slice(index, length)
      packet.content.readerIndex(index + length)

      logger.debug("index " + i)
      fields(i) = slice
      logger.debug("bytes " + length)
      packet.content.readerIndex(index + length)
    }

    Some(new DataRow(fields))
  }

  def parseT(packet: Packet): Option[BackendMessage] = {

    logger.debug("parsing T")
    logger.debug("Packet " + packet)

    val fieldNumber = packet.content.readShort

    val fields = new Array[FieldDescription](fieldNumber)
    logger.debug(fieldNumber + " fields")
    0.until(fieldNumber).foreach { index =>

      val field = new FieldDescription(
        Buffers.readCString(packet.content),
        packet.content.readInt,
        packet.content.readShort,
        packet.content.readInt,
        packet.content.readShort,
        packet.content.readInt,
        packet.content.readShort)

      logger.debug("field " + field)
      fields(index) = field
    }

    Some(new RowDescription(fields))
  }

  def parseC(packet: Packet) : Option[BackendMessage] = {
	  logger.debug("Parsing C")
	  logger.debug("Packet " + packet)
	  
	  val tag = Buffers.readCString(packet.content)
	  
	  Some(new CommandComplete(tag))
  }

  def parseS(packet: Packet): Option[BackendMessage] = {
    logger.debug("Parsing S " + packet)
    val parameterStatus = new ParameterStatus(Buffers.readCString(packet.content), Buffers.readCString(packet.content))
    logger.debug("Sync " + parameterStatus)
    Some(parameterStatus)
  }

}