package com.twitter.finagle.postgres.protocol


import org.jboss.netty.buffer.ChannelBuffer

import com.twitter.logging.Logger

import Convert._

trait Message

/**
 * Response sent from Postgres back to the client.
 */
trait BackendMessage extends Message

case class ErrorResponse(msg: Option[String] = None) extends BackendMessage

case class NoticeResponse(msg: Option[String]) extends BackendMessage

case class NotificationResponse(processId: Int, channel: String, payload: String) extends BackendMessage

case class AuthenticationOk() extends BackendMessage

case class AuthenticationMD5Password(salt: Array[Byte]) extends BackendMessage

case class AuthenticationCleartextPassword() extends BackendMessage

case class ParameterStatus(name: String, value: String) extends BackendMessage

case class BackendKeyData(processId: Int, secretKey: Int) extends BackendMessage

case class ParameterDescription(types: IndexedSeq[Int]) extends BackendMessage

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

sealed trait CommandCompleteStatus

case object CreateTable extends CommandCompleteStatus

case object DropTable extends CommandCompleteStatus

case class Insert(count : Int) extends CommandCompleteStatus

case class Update(count : Int) extends CommandCompleteStatus

case class Delete(count : Int) extends CommandCompleteStatus

case class Select(count: Int) extends CommandCompleteStatus

case class CommandComplete(status: CommandCompleteStatus) extends BackendMessage

case class ReadyForQuery(status: Char) extends BackendMessage

case object ParseComplete extends BackendMessage

case object BindComplete extends BackendMessage

case object NoData extends BackendMessage

case object PortalSuspended extends BackendMessage

case object EmptyQueryResponse extends BackendMessage

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
        parseI(packet)
      case 'V' =>
        logger.error("'V' Not implemented yet")
        throw new UnsupportedOperationException("'V' Not implemented yet")
      case 'n' =>
        parseSmallN(packet)
      case 'N' =>
        parseN(packet)
      case 'A' =>
        parseA(packet)
      case 't' =>
        parseSmallT(packet)
      case 'S' =>
        parseS(packet)
      case '1' =>
        parse1(packet)
      case '2' =>
        parse2(packet)
      case 's' =>
        parseSmallS(packet)
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

    val Packet(_, _, content) = packet

    val builder = new StringBuilder()
    while (content.readable) {
      builder.append(Buffers.readCString(content))
    }

    Some(new ErrorResponse(Some(builder.toString)))
  }

  def parseN(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing N")
    logger.ifDebug("Packet " + packet)

    val Packet(_, _, content) = packet

    val builder = new StringBuilder()
    while (content.readable) {
      builder.append(Buffers.readCString(content))
    }

    Some(new NoticeResponse(Some(builder.toString)))
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

    val Packet(_, _, content) = packet

    val fieldNumber = content.readShort
    logger.ifDebug("fields " + fieldNumber)
    val fields = new Array[ChannelBuffer](fieldNumber)

    for (i <- 0.until(fieldNumber)) {
        val length = content.readInt
        if (length == -1) {
          fields(i) = null
        } else {
          val index = content.readerIndex
          val slice = content.slice(index, length)
          content.readerIndex(index + length)

          logger.ifDebug("index " + i)
          fields(i) = slice
          logger.ifDebug("bytes " + length)
          content.readerIndex(index + length)
        }
    }

    Some(new DataRow(fields))
  }

  def parseT(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing T")
    logger.ifDebug("Packet " + packet)

    val Packet(_, _, content) = packet

    val fieldNumber = content.readShort

    val fields = new Array[FieldDescription](fieldNumber)
    logger.ifDebug(fieldNumber + " fields")
    0.until(fieldNumber).foreach {
      index =>

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

    val Packet(_, _, content) = packet

    val tag = Buffers.readCString(content)

    Some(new CommandComplete(parseTag(tag)))
  }

  def parseTag(tag: String) : CommandCompleteStatus = {
    if (tag == "CREATE TABLE") {
      CreateTable
    } else if (tag == "DROP TABLE") {
      DropTable
    } else {
      val parts = tag.split(" ")

      parts(0) match {
        case "SELECT" => Select(parts(1).toInt)
        case "INSERT" => Insert(parts(2).toInt)
        case "DELETE" => Delete(parts(1).toInt)
        case "UPDATE" => Update(parts(1).toInt)
        case _ => throw new IllegalStateException("Unknown command complete response tag " + tag)
      }
    }
  }

  def parseS(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("Parsing S " + packet)
    val Packet(_, _, content) = packet

    val parameterStatus = new ParameterStatus(Buffers.readCString(content), Buffers.readCString(content))
    logger.ifDebug("ParameterStatus " + parameterStatus)
    Some(parameterStatus)
  }

  def parse1(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("Parsing 1 " + packet)

    Some(ParseComplete)
  }

  def parse2(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("Parsing 2 " + packet)

    Some(BindComplete)
  }

  def parseSmallT(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing t")
    logger.ifDebug("Packet " + packet)

    val Packet(_, _, content) = packet

    val paramNumber = content.readShort

    val params = new Array[Int](paramNumber)
    0.until(paramNumber).foreach {
      index =>
        val param = content.readInt
        logger.ifDebug("param " + param)
        params(index) = param
    }

    Some(new ParameterDescription(params))

  }

  def parseSmallN(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing n" + packet)
    Some(NoData)
  }

  def parseSmallS(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing s" + packet)
    Some(PortalSuspended)
  }

  def parseI(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing I " + packet)
    Some(EmptyQueryResponse)
  }

  def parseA(packet: Packet): Option[BackendMessage] = {
    logger.ifDebug("parsing A " + packet)
    val Packet(_, _, content) = packet
    val processId = content.readInt
    val channel = Buffers.readCString(content)
    val payload = Buffers.readCString(content)

    Some(NotificationResponse(processId, channel, payload))
  }

}

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

case class Parse(name: String = Strings.empty, query: String = "", paramTypes: Seq[Int] = Seq()) extends FrontendMessage {

  def asPacket() = {
    val builder = PacketBuilder('P')
      .writeCString(name)
      .writeCString(query)
      .writeShort(asShort(paramTypes.length))

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
      for (format <- formats) {
        builder.writeShort(format.toShort)
      }
    }

    builder.writeShort(asShort(params.length))

    for (param <- params) {
      if (param.readableBytes == 4 && param.getInt(0) == -1) {
        builder.writeInt(-1)  // NULL.
      } else {
        builder.writeInt(param.readableBytes)
        builder.writeBuf(param)
      }
    }

    if (resultFormats.isEmpty) {
      builder.writeShort(0)
    } else {
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
