package com.twitter.finagle.postgres.protocol

import scala.collection.mutable.MutableList

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers

import com.twitter.logging.Logger

/**
 * Response sent from Postgres back to the client.
 */
trait BackendMessage

case class ErrorResponse(msg: Option[String]) extends BackendMessage

case class AuthenticationOk() extends BackendMessage

case class AuthenticationMD5Password(salt: Array[Byte]) extends BackendMessage

case class AuthenticationCleartextPassword() extends BackendMessage

case class ParameterStatus(name: String, value: String) extends BackendMessage

case class BackendKeyData(processId: Int, secretKey: Int) extends BackendMessage

case class RowDescription(fields: IndexedSeq[FieldDescription]) extends BackendMessage

case class FieldDescription(
  name: String,
  tableId: Int,
  columnNumber: Int,
  dataType: Int,
  dataTypeSize: Int,
  dataTypeMondifier: Int,
  fieldFormat: Int)

case class DataRow(data: IndexedSeq[ChannelBuffer]) extends BackendMessage

case class CommandComplete(tag: String) extends BackendMessage

case class ReadyForQuery(status: Char) extends BackendMessage

class BackendMessageParser {

  private val logger = Logger(getClass.getName)

  def parse(packet: Packet): Option[BackendMessage] = {
    
    val result: Option[BackendMessage] = packet.code.get match {
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

    logger.ifDebug("Result " + result)
    result
  }

  def parseR(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing R")
    logger.ifDebug("Packet " + packet)

    val Packet(_, length, content) = packet

    length match {
      case 8 =>
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
        val code = packet.content.readInt
        if (code == 5) {
          val salt = new Array[Byte](4)
          packet.content.readBytes(salt)
          Some(new AuthenticationMD5Password(salt))
        } else {
          None
        }

      case _ =>
        None
    }

  }

  def parseE(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing E")
    logger.ifDebug("Packet " + packet)

    val Packet(_, length, content) = packet

    val builder = new StringBuilder()
    while (content.readable) {
      builder.append(Buffers.readCString(content))
    }

    Some(new ErrorResponse(Some(builder.toString)))
  }

  def parseZ(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing Z")
    logger.ifDebug("Packet " + packet)

    val Packet(_, length, content) = packet

    if (length != 5) {
      throw new IllegalStateException("Unexpected message length ")
    }

    val status = content.readByte().asInstanceOf[Char]

    Some(new ReadyForQuery(status))

  }

  def parseK(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing K")
    logger.ifDebug("Packet " + packet)

    val Packet(_, length, content) = packet

    if (length != 12) {
      throw new IllegalStateException("Unexpected message length ")
    }

    val processId = content.readInt
    val secretKey = content.readInt

    Some(new BackendKeyData(processId, secretKey))
  }

  def parseSmallD(packet: Packet): Option[BackendMessage] = {
    throw new UnsupportedOperationException("'d' Not implemented yet")
  }

  def parseD(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing D")
    logger.ifDebug("Packet " + packet)

    val Packet(_, length, content) = packet

    val fieldNumber = content.readShort
    logger.ifDebug("fields " + fieldNumber)
    val fields = new Array[ChannelBuffer](fieldNumber)

    0.until(fieldNumber).foreach { i =>
      val length = content.readInt
      val index = content.readerIndex
      val slice = content.slice(index, length)
      content.readerIndex(index + length)

      logger.ifDebug("index " + i)
      fields(i) = slice
      logger.ifDebug("bytes " + length)
      content.readerIndex(index + length)
    }

    Some(new DataRow(fields))
  }

  def parseT(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing T")
    logger.ifDebug("Packet " + packet)

    val Packet(_, length, content) = packet

    val fieldNumber = content.readShort

    val fields = new Array[FieldDescription](fieldNumber)
    logger.ifDebug(fieldNumber + " fields")
    0.until(fieldNumber).foreach { index =>

      val field = new FieldDescription(
        Buffers.readCString(content),
        content.readInt,
        content.readShort,
        content.readInt,
        content.readShort,
        content.readInt,
        content.readShort)

      logger.ifDebug("field " + field)
      fields(index) = field
    }

    Some(new RowDescription(fields))
  }

  def parseC(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("Parsing C")
    logger.ifDebug("Packet " + packet)

    val Packet(_, length, content) = packet

    val tag = Buffers.readCString(content)

    Some(new CommandComplete(tag))
  }

  def parseS(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("Parsing S " + packet)
    val Packet(_, length, content) = packet

    val parameterStatus = new ParameterStatus(Buffers.readCString(content), Buffers.readCString(content))
    logger.ifDebug("Sync " + parameterStatus)
    Some(parameterStatus)
  }

}