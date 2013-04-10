package com.twitter.finagle.exp.mysql

import com.twitter.logging.Logger
import com.twitter.finagle.exp.mysql.protocol.{Command, Packet, Buffer, BufferWriter, Type}
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers._
import scala.annotation.tailrec

abstract class Request(seq: Short) {
  /**
   * Request data translates to the body of the MySQL
   * Packet sent to the server. This field becomes
   * part of a compisition of ChannelBuffers. To ensure
   * that it has the correct byte order use Buffer.toChannelBuffer(...)
   * to create the ChannelBuffer.
   */
  val data: ChannelBuffer

  def toChannelBuffer: ChannelBuffer =
    Packet.toChannelBuffer(data.capacity, seq, data)
}

abstract class CommandRequest(val cmd: Byte) extends Request(0)

class SimpleCommandRequest(command: Byte, buffer: Array[Byte])
  extends CommandRequest(command) {
    override val data = Buffer.toChannelBuffer(Array(cmd), buffer)
}

/**
 * NOOP Request used internally by this client.
 */
case object ClientInternalGreet extends Request(0) {
  override val data = EMPTY_BUFFER
  override def toChannelBuffer = EMPTY_BUFFER
}

case object PingRequest
  extends SimpleCommandRequest(Command.COM_PING, Buffer.EMPTY_BYTE_ARRAY)

case class UseRequest(dbName: String)
  extends SimpleCommandRequest(Command.COM_INIT_DB, dbName.getBytes)

case class QueryRequest(sqlStatement: String)
  extends SimpleCommandRequest(Command.COM_QUERY, sqlStatement.getBytes)

case class PrepareRequest(sqlStatement: String)
  extends SimpleCommandRequest(Command.COM_STMT_PREPARE, sqlStatement.getBytes)

/**
 * An Execute Request.
 * Uses the binary protocol to build an execute request for
 * a prepared statement.
 */
case class ExecuteRequest(ps: PreparedStatement, flags: Byte = 0, iterationCount: Int = 1)
  extends CommandRequest(Command.COM_STMT_EXECUTE) {
    private[this] val log = Logger("finagle-mysql")

    private[this] def isNull(param: Any): Boolean = param match {
      case null => true
      case _ => false
    }

    private[this] def makeNullBitmap(parameters: List[Any]): Array[Byte] = {
      val bitmap = new Array[Byte]((parameters.size + 7) / 8)
      @tailrec
      def fill(params: List[Any], pos: Int): Array[Byte] = params match {
        case Nil => bitmap
        case param :: rest =>
          if (isNull(param)) {
            val bytePos = pos / 8
            val bitPos = pos % 8
            val byte = bitmap(bytePos)
            bitmap(bytePos) = (byte | (1 << bitPos)).toByte
          }
          fill(rest, pos+1)
      }

      fill(parameters, 0)
    }

    private[this] def writeTypeCode(param: Any, writer: BufferWriter): Unit = {
      val typeCode = Type.getCode(param)

      if (typeCode != -1)
        writer.writeShort(typeCode)
      else {
        // Unsupported type. Write the error to log, and write the type as null.
        // This allows us to safely skip writing the parameter without corrupting the buffer.
        log.error("ExecuteRequest: Unknown parameter %s will be treated as SQL NULL.".format(param.getClass.getName))
        writer.writeShort(Type.NULL)
      }
    }

    /**
     * Returns sizeof all the parameters in the List.
     */
    @tailrec
    private[this] def sizeOfParameters(parameters: List[Any], size: Int = 0): Int = parameters match {
      case Nil => size
      case p :: rest =>
        val typeSize = Type.sizeOf(p)
        // We can safely convert unknown sizes to 0 because
        // any unknown type is being sent as NULL.
        val sizeOfParam = if (typeSize == -1) 0 else typeSize
        sizeOfParameters(rest, size + sizeOfParam)
    }

    /**
     * Writes the parameter into its MySQL binary representation.
     */
    private[this] def writeParam(param: Any, writer: BufferWriter) = param match {
      // TODO: defaults to UTF-8.  this is ok for ascii, not for every encoding.
      case s: String      => writer.writeLengthCodedString(s)
      case b: Boolean     => writer.writeBoolean(b)
      case b: Byte        => writer.writeByte(b)
      case s: Short       => writer.writeShort(s)
      case i: Int         => writer.writeInt(i)
      case l: Long        => writer.writeLong(l)
      case f: Float       => writer.writeFloat(f)
      case d: Double      => writer.writeDouble(d)
      case b: Array[Byte] => writer.writeLengthCodedBytes(b)
      // Dates
      case t: java.sql.Timestamp    => TimestampValue.write(t, writer)
      case d: java.sql.Date         => DateValue.write(d, writer)
      case d: java.util.Date        => TimestampValue.write(new java.sql.Timestamp(d.getTime), writer)
      case _  => writer // skip null and uknown values
    }

    override val data = {
      val bw = BufferWriter(new Array[Byte](10))
      bw.writeByte(cmd)
      bw.writeInt(ps.statementId)
      bw.writeByte(flags)
      bw.writeInt(iterationCount)

      val paramsList = ps.parameters.toList
      val nullBytes = makeNullBitmap(paramsList)
      val newParamsBound: Byte = if (ps.hasNewParameters) 1 else 0

      val initialBuffer = Buffer.toChannelBuffer(bw.array, nullBytes, Array(newParamsBound))

      // convert parameters to binary representation.
      val sizeOfParams = sizeOfParameters(paramsList)
      val values = BufferWriter(new Array[Byte](sizeOfParams))
      paramsList foreach { writeParam(_, values) }

      // parameters are tagged on to the end of the buffer
      // after types or initialBuffer depending if the prepared statement
      // has new parameters.
      if (ps.hasNewParameters) {
        // only add type data if the prepared statement has new parameters.
        val types = BufferWriter(new Array[Byte](ps.numberOfParams * 2))
        paramsList foreach { writeTypeCode(_, types) }
        wrappedBuffer(initialBuffer, types.toChannelBuffer, values.toChannelBuffer)
      } else {
        wrappedBuffer(initialBuffer, values.toChannelBuffer)
      }
    }
}

case class CloseRequest(ps: PreparedStatement) extends CommandRequest(Command.COM_STMT_CLOSE) {
  override val data = {
    val bw = BufferWriter(new Array[Byte](5))
    bw.writeByte(cmd).writeInt(ps.statementId)
    bw.toChannelBuffer
  }
}
