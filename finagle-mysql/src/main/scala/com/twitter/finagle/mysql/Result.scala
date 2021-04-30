package com.twitter.finagle.mysql

import com.twitter.finagle.mysql.transport.{MysqlBuf, Packet}
import com.twitter.io.Buf
import com.twitter.util.{Return, Try}

import scala.collection.immutable.IndexedSeq

sealed trait Result

/**
 * A decoder for Results contained in a single packet.
 */
trait Decoder[T <: Result] extends (Packet => Try[T]) {
  def apply(packet: Packet): Try[T] = Try(decode(packet))
  def decode(packet: Packet): T
}

/**
 * First result received from the server as part of the connection phase.
 * [[https://dev.mysql.com/doc/internals/en/connection-phase-packets.html]]
 */
object HandshakeInit extends Decoder[HandshakeInit] {
  def decode(packet: Packet): HandshakeInit = {
    val br = MysqlBuf.reader(packet.body)

    try {
      // If an error occurs during the handshake, the first packet will tell us.
      // Otherwise the first packet will be the protocol packet.
      br.readByte() match {
        case Packet.ErrorByte =>
          val code: Int = br.readShortLE()
          throw new Exception(s"Exception in MySQL handshake, error code $code")
        case protocol =>
          val bytesVersion = br.readNullTerminatedBytes()
          val threadId = br.readIntLE()
          val salt1 = Buf.ByteArray.Owned.extract(br.readBytes(8))
          br.skip(1) // 1 filler byte always 0x00

          // the rest of the fields are optional and protocol version specific
          val capLow = if (br.remaining >= 2) br.readUnsignedShortLE() else 0

          require(
            protocol == 10 && (capLow & Capability.Protocol41) != 0,
            "unsupported protocol version"
          )

          val charset = br.readUnsignedByte()
          val status = br.readShortLE()
          val capHigh = br.readUnsignedShortLE() << 16
          val serverCap = Capability(capHigh, capLow)
          val hasPluginAuthCap = serverCap.has(Capability.PluginAuth)

          val authPluginDataLen = br.readUnsignedByte() // sometimes 0

          // next 10 bytes are all reserved
          br.readBytes(10)

          val salt2 =
            if (hasPluginAuthCap && authPluginDataLen != 0) {
              // Documentation says 13 or authPluginDataLen - 8
              br.readNullTerminatedBytes()
            } else {
              if (!serverCap.has(Capability.SecureConnection)) Array.empty[Byte]
              else br.readNullTerminatedBytes()
            }

          HandshakeInit(
            protocol,
            new String(bytesVersion, MysqlCharset(charset)),
            threadId,
            Array.concat(salt1, salt2),
            serverCap,
            charset,
            status
          )
      }
    } finally br.close()
  }
}

case class HandshakeInit(
  protocol: Byte,
  version: String,
  threadId: Int,
  salt: Array[Byte],
  serverCapabilities: Capability,
  charset: Short,
  status: Short)
    extends Result

/**
 * Sent during the connection phase if the client and server both support pluggable
 * authentication, that is, they both have [[Capability.PluginAuth]]. Sent to switch
 * the authentication method.
 *
 * @see [[https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html]]
 */
private[mysql] object AuthSwitchRequest extends Decoder[AuthSwitchRequest] {
  def decode(packet: Packet): AuthSwitchRequest = {
    val br = MysqlBuf.reader(packet.body)
    try {
      // 0xfe status byte doubles as the
      // AuthSwitchRequest packet identifier
      br.readByte()

      val pluginName = br.readNullTerminatedString()
      val pluginData = br.readNullTerminatedBytes()
      AuthSwitchRequest(
        seqNum = packet.seq,
        pluginName = pluginName,
        pluginData = pluginData
      )
    } finally br.close()
  }
}

private[mysql] case class AuthSwitchRequest(
  seqNum: Short,
  pluginName: String,
  pluginData: Array[Byte])
    extends Result

/**
 * Sent between the client and the server during the connection phase in order
 * to pass information required for authentication.
 *
 * @see [[https://dev.mysql.com/doc/dev/mysql-server/8.0.22/page_protocol_connection_phase_packets_protocol_auth_more_data.html]]
 */
private[mysql] object AuthMoreDataFromServer extends Decoder[AuthMoreDataFromServer] {
  def decode(packet: Packet): AuthMoreDataFromServer = {
    val br = MysqlBuf.reader(packet.body)
    try {
      // 0x01 status byte to identify the AuthMoreData packet
      br.readByte()

      val firstByteBuf = br.readBytes(1)
      val firstByte = firstByteBuf.get(0)
      // either one byte or the server's public key
      firstByte match {
        case FastAuthSuccess.moreDataByte =>
          AuthMoreDataFromServer(packet.seq, FastAuthSuccess, None)
        case PerformFullAuth.moreDataByte =>
          AuthMoreDataFromServer(packet.seq, PerformFullAuth, None)
        case _ => // server sent public key as contents of packet
          val bufToConvertToString = firstByteBuf.concat(br.readAll())
          val bytesToConvertToString = new Array[Byte](packet.size)
          bufToConvertToString.write(bytesToConvertToString, 0)
          AuthMoreDataFromServer(packet.seq, NeedPublicKey, Some(bytesToConvertToString))
      }
    } finally br.close()
  }
}

private[mysql] case class AuthMoreDataFromServer(
  seqNum: Short,
  moreDataType: AuthMoreDataType,
  authData: Option[Array[Byte]])
    extends Result

object OK extends Decoder[OK] {

  /**
   * @see [[https://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet]]
   */
  def decode(packet: Packet): OK = {
    val br = MysqlBuf.reader(packet.body)
    try {
      br.skip(1)
      OK(
        br.readVariableLong(),
        br.readVariableLong(),
        br.readUnsignedShortLE(),
        br.readUnsignedShortLE(), {
          val remaining = br.remaining
          if (remaining == 0) ""
          else new String(br.take(remaining))
        }
      )
    } finally br.close()
  }
}

/**
 * Represents the OK Packet received from the server. It is sent
 * to indicate that a command (e.g. [[PreparedStatement.modify]])
 * has completed successfully.
 *
 * @param affectedRows how many records were changed by the command.
 *
 * @param insertId the first automatically generated value successfully
 *                 inserted for an AUTO_INCREMENT column for an INSERT statement.
 *
 * @param serverStatus server status bit mask.
 * @param warningCount how many warnings were generated.
 * @param message the status message, which will be an empty String if none is present.
 */
case class OK(
  affectedRows: Long,
  insertId: Long,
  serverStatus: Int,
  warningCount: Int,
  message: String)
    extends Result

/**
 * Represents the Error Packet received from the server and the data sent along with it.
 * [[https://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-ERR_Packet]]
 */
object Error extends Decoder[Error] {
  def decode(packet: Packet): Error = {
    // start reading after flag byte
    val br = MysqlBuf.reader(packet.body)
    try {
      br.skip(1)
      val code = br.readShortLE()
      val state = new String(br.take(6))
      val msg = new String(br.take(br.remaining))
      Error(code, state, msg)
    } finally br.close()
  }
}

case class Error(code: Short, sqlState: String, message: String) extends Result

/**
 * Represents and EOF result received from the server which
 * contains any warnings and the server status.
 * [[https://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet]]
 */
object EOF extends Decoder[EOF] {
  def decode(packet: Packet): EOF = {
    val br = MysqlBuf.reader(packet.body)
    try {
      br.skip(1)
      EOF(br.readShortLE(), ServerStatus(br.readShortLE()))
    } finally br.close()
  }
}

case class EOF(warnings: Short, serverStatus: ServerStatus) extends Result

/**
 * These bit masks are to understand whether corresponding attribute
 * is set for the field. Link to source code from mysql is below.
 * [[https://github.com/mysql/mysql-server/blob/5.7/include/mysql_com.h]]
 */
object FieldAttributes {
  val NotNullBitMask: Short = 1
  val PrimaryKeyBitMask: Short = 2
  val UniqueKeyBitMask: Short = 4
  val MultipleKeyBitMask: Short = 8
  val BlobBitMask: Short = 16
  val UnsignedBitMask: Short = 32
  val ZeroFillBitMask: Short = 64
  val BinaryBitMask: Short = 128
}

/**
 * Represents the column meta-data associated with a query.
 * Sent during ResultSet transmission and as part of the
 * meta-data associated with a Row.
 * [[https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41]]
 */
object Field extends Decoder[Field] {
  def decode(packet: Packet): Field = {
    val br = MysqlBuf.reader(packet.body)
    try {
      val bytesCatalog = br.readLengthCodedBytes()
      val bytesDb = br.readLengthCodedBytes()
      val bytesTable = br.readLengthCodedBytes()
      val bytesOrigTable = br.readLengthCodedBytes()
      val bytesName = br.readLengthCodedBytes()
      val bytesOrigName = br.readLengthCodedBytes()
      br.readVariableLong() // length of the following fields (always 0x0c)
      val charset = br.readShortLE()
      val jCharset = MysqlCharset(charset)
      val catalog = new String(bytesCatalog, jCharset)
      val db = new String(bytesDb, jCharset)
      val table = new String(bytesTable, jCharset)
      val origTable = new String(bytesOrigTable, jCharset)
      val name = new String(bytesName, jCharset)
      val origName = new String(bytesOrigName, jCharset)
      val length = br.readIntLE()
      val fieldType = br.readUnsignedByte()
      val flags = br.readShortLE()
      val decimals = br.readByte()
      Field(
        catalog,
        db,
        table,
        origTable,
        name,
        origName,
        charset,
        length,
        fieldType,
        flags,
        decimals
      )
    } finally br.close()
  }
}

case class Field(
  catalog: String,
  db: String,
  table: String,
  origTable: String,
  name: String,
  origName: String,
  charset: Short,
  displayLength: Int,
  fieldType: Short,
  flags: Short,
  decimals: Byte)
    extends Result {
  def id: String = if (name.isEmpty) origName else name
  override def toString: String = s"Field($id)"

  def isUnsigned: Boolean = (flags & FieldAttributes.UnsignedBitMask) > 0
  def isSigned: Boolean = !isUnsigned
}

/**
 * Meta data returned from the server in response to
 * a prepared statement initialization request
 * COM_STMT_PREPARE.
 * [[https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK]]
 */
object PrepareOK extends Decoder[PrepareOK] {
  def decode(header: Packet): PrepareOK = {
    val br = MysqlBuf.reader(header.body)
    try {
      br.skip(1)
      val stmtId = br.readIntLE()
      val numCols = br.readUnsignedShortLE()
      val numParams = br.readUnsignedShortLE()
      br.skip(1)
      val warningCount = br.readUnsignedShortLE()
      PrepareOK(stmtId, numCols, numParams, warningCount)
    } finally br.close()
  }
}

case class PrepareOK(
  id: Int,
  numOfCols: Int,
  numOfParams: Int,
  warningCount: Int,
  columns: Seq[Field] = Nil,
  params: Seq[Field] = Nil)
    extends Result

/**
 * Used internally to synthesize a response from
 * the server when sending a prepared statement
 * CloseRequest
 */
object CloseStatementOK extends OK(0, 0, 0, 0, "Internal Close OK")

/**
 * Resultset returned from the server containing field definitions and
 * rows. The rows can be binary encoded (for prepared statements)
 * or text encoded (for regular queries).
 * [[https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset]]
 * [[https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html]]
 */
private object ResultSetBuilder {
  def apply(
    isBinaryEncoded: Boolean,
    supportUnsigned: Boolean
  )(
    header: Packet,
    fieldPackets: Seq[Packet],
    rowPackets: Seq[Packet]
  ): Try[ResultSet] =
    Try(decode(isBinaryEncoded, supportUnsigned)(header, fieldPackets, rowPackets))

  def decode(
    isBinaryEncoded: Boolean,
    supportUnsigned: Boolean
  )(
    header: Packet,
    fieldPackets: Seq[Packet],
    rowPackets: Seq[Packet]
  ): ResultSet = {
    val fields = fieldPackets.map(Field.decode).toIndexedSeq

    decodeRows(isBinaryEncoded, supportUnsigned, rowPackets, fields)
  }

  private[this] def decodeRows(
    isBinaryEncoded: Boolean,
    supportUnsigned: Boolean,
    rowPackets: Seq[Packet],
    fields: IndexedSeq[Field]
  ): ResultSet = {
    // A name -> index map used to allow quick lookups for rows based on name.
    val indexMap = fields.map(_.id).zipWithIndex.toMap

    /**
     * Rows can be encoded as Strings or Binary depending
     * on if the ResultSet is created by a normal query or
     * a prepared statement, respectively.
     */
    val rows = rowPackets.map { p: Packet =>
      if (!isBinaryEncoded)
        new StringEncodedRow(p.body, fields, indexMap, !supportUnsigned)
      else
        new BinaryEncodedRow(p.body, fields, indexMap, !supportUnsigned)
    }

    ResultSet(fields, rows)
  }
}

case class ResultSet(fields: Seq[Field], rows: Seq[Row]) extends Result {
  override def toString = s"ResultSet(${fields.size}, ${rows.size})"
}

object FetchResult {
  def apply(rowPackets: Seq[Packet], eofPacket: EOF): Try[FetchResult] = {
    Try {
      val containsLastRow: Boolean = eofPacket.serverStatus.has(ServerStatus.LastRowSent)
      FetchResult(rowPackets, containsLastRow)
    }
  }

  def apply(err: Error): Try[FetchResult] = {
    Return(FetchResult(Seq(), containsLastRow = true))
  }
}

case class FetchResult(rowPackets: Seq[Packet], containsLastRow: Boolean) extends Result {
  override def toString: String =
    s"FetchResult(rows=${rowPackets.size}, containsLastRow=$containsLastRow)"
}
