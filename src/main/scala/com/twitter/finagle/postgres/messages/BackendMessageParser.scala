package com.twitter.finagle.postgres.messages

import com.twitter.finagle.postgres.values.Buffers
import com.twitter.logging.Logger

import org.jboss.netty.buffer.ChannelBuffer

/*
 * Class for converting packets into BackendMessages.
 */
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

    result
  }

  def parseR(packet: Packet): Option[BackendMessage] = {
    val Packet(_, length, content, _) = packet

    length match {
      case 8 =>
        val code = content.readInt()

        code match {
          case 3 =>
            Some(AuthenticationCleartextPassword())
          case 0 =>
            Some(AuthenticationOk())
          case _ =>
            None
        }

      case 12 =>
        val code = packet.content.readInt
        if (code == 5) {
          val salt = new Array[Byte](4)
          packet.content.readBytes(salt)
          Some(AuthenticationMD5Password(salt))
        } else {
          None
        }

      case _ =>
        None
    }
  }

  def parseE(packet: Packet): Option[BackendMessage] = {
    val Packet(_, _, content, _) = packet

    def fieldStream(buf: ChannelBuffer):Stream[(Char,String)] =
      if(buf.readable)
        Buffers.readCString(buf) match {
          case "" => Stream.empty
          case nonempty => (nonempty.splitAt(1) match { case (fieldId, value) => (fieldId.charAt(0), value) }) #:: fieldStream(buf)
        }
      else Stream.empty[(Char,String)]

    val fields = fieldStream(content).foldLeft(Map.empty[Char,String])(_ + _)

    Some(ErrorResponse(fields))
  }

  def parseN(packet: Packet): Option[BackendMessage] = {
    val Packet(_, _, content, inSslNegotation) = packet

    if (inSslNegotation) {
      Some(SslNotSupported)
    } else {
      val builder = new StringBuilder()
      while (content.readable) {
        builder.append(Buffers.readCString(content))
      }

      Some(NoticeResponse(Some(builder.toString)))
    }
  }

  def parseZ(packet: Packet): Option[BackendMessage] = {
    val Packet(_, length, content, _) = packet

    if (length != 5) {
      throw new IllegalStateException("Unexpected message length ")
    }

    val status = content.readByte().asInstanceOf[Char]

    Some(ReadyForQuery(status))
  }

  def parseK(packet: Packet): Option[BackendMessage] = {
    val Packet(_, length, content, _) = packet

    if (length != 12) {
      throw new IllegalStateException("Unexpected message length ")
    }

    val processId = content.readInt
    val secretKey = content.readInt

    Some(BackendKeyData(processId, secretKey))
  }

  def parseSmallD(packet: Packet): Option[BackendMessage] = {
    throw new UnsupportedOperationException("'d' Not implemented yet")
  }

  def parseD(packet: Packet): Option[BackendMessage] = {
    val Packet(_, _, content, _) = packet

    val fieldNumber = content.readShort
    val fields = new Array[Option[ChannelBuffer]](fieldNumber)

    for (i <- 0.until(fieldNumber)) {
      val length = content.readInt
      if (length == -1) {
        fields(i) = None
      } else {
        val index = content.readerIndex
        val slice = content.slice(index, length)
        content.readerIndex(index + length)

        fields(i) = Some(slice)

        content.readerIndex(index + length)
      }
    }

    Some(DataRow(fields))
  }

  def parseT(packet: Packet): Option[BackendMessage] = {
    val Packet(_, _, content, _) = packet

    val fieldNumber = content.readShort

    val fields = new Array[FieldDescription](fieldNumber)
    0.until(fieldNumber).foreach {
      index =>

        val field = FieldDescription(
          Buffers.readCString(content),
          content.readInt,
          content.readShort,
          content.readInt,
          content.readShort,
          content.readInt,
          content.readShort)

        fields(index) = field
    }

    Some(RowDescription(fields))
  }

  def parseC(packet: Packet): Option[BackendMessage] = {
    val Packet(_, _, content, _) = packet

    val tag = Buffers.readCString(content)

    Some(CommandComplete(parseTag(tag)))
  }

  def parseTag(tag: String) : CommandCompleteStatus = {
    tag match {
      case "CREATE TABLE" => CreateTable
      case "CREATE EXTENSION"=> CreateExtension
      case "CREATE TYPE" => CreateType
      case "DO" => Do
      case "DISCARD ALL" => DiscardAll
      case "DROP TABLE" => DropTable
      case _ =>
        val parts = tag.split(" ")

        parts(0) match {
          case "SELECT" => Select(parts(1).toInt)
          case "INSERT" => Insert(parts(2).toInt)
          case "DELETE" => Delete(parts(1).toInt)
          case "UPDATE" => Update(parts(1).toInt)
          case "BEGIN"  => Begin
          case "SAVEPOINT" => Savepoint
          case "ROLLBACK"  => RollBack
          case "COMMIT" => Commit
          case _ => throw new IllegalStateException("Unknown command complete response tag " + tag)
        }
    }
  }

  def parseS(packet: Packet): Option[BackendMessage] = {
    val Packet(_, _, content, inSslNegotation) = packet

    if (inSslNegotation) {
      Some(SwitchToSsl)
    } else {
      val parameterStatus = ParameterStatus(Buffers.readCString(content), Buffers.readCString(content))
      Some(parameterStatus)
    }
  }

  def parse1(packet: Packet): Option[BackendMessage] = {
    Some(ParseComplete)
  }

  def parse2(packet: Packet): Option[BackendMessage] = {
    Some(BindComplete)
  }

  def parseSmallT(packet: Packet): Option[BackendMessage] = {
    val Packet(_, _, content, _) = packet

    val paramNumber = content.readShort

    val params = new Array[Int](paramNumber)
    0.until(paramNumber).foreach {
      index =>
        val param = content.readInt
        params(index) = param
    }

    Some(ParameterDescription(params))

  }

  def parseSmallN(packet: Packet): Option[BackendMessage] = {
    Some(NoData)
  }

  def parseSmallS(packet: Packet): Option[BackendMessage] = {
    Some(PortalSuspended)
  }

  def parseI(packet: Packet): Option[BackendMessage] = {
    Some(EmptyQueryResponse)
  }

  def parseA(packet: Packet): Option[BackendMessage] = {
    val Packet(_, _, content, _) = packet
    val processId = content.readInt
    val channel = Buffers.readCString(content)
    val payload = Buffers.readCString(content)

    Some(NotificationResponse(processId, channel, payload))
  }
}
