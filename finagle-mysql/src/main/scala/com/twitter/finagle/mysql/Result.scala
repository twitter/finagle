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
 * [[http://dev.mysql.com/doc/internals/en/connection-phase-packets.html]]
 */
object HandshakeInit extends Decoder[HandshakeInit] {
  def decode(packet: Packet) = {
    val br = MysqlBuf.reader(packet.body)
    val protocol = br.readByte()
    val bytesVersion = br.readNullTerminatedBytes()
    val threadId = br.readIntLE()
    val salt1 = Buf.ByteArray.Owned.extract(br.readBytes(8))
    br.skip(1) // 1 filler byte always 0x00

    // the rest of the fields are optional and protocol version specific
    val capLow = if (br.remaining >= 2) br.readUnsignedShortLE() else 0

    require(protocol == 10 && (capLow & Capability.Protocol41) != 0,
      "unsupported protocol version")

    val charset = br.readUnsignedByte().toShort
    val status = br.readShortLE().toShort
    val capHigh = br.readUnsignedShortLE() << 16
    val serverCap = Capability(capHigh, capLow)

    // auth plugin data. Currently unused but we could verify
    // that our secure connections respect the expected size.
    br.skip(1)

    // next 10 bytes are all reserved
    br.readBytes(10)

    val salt2 =
      if (!serverCap.has(Capability.SecureConnection)) Array.empty[Byte]
      else br.readNullTerminatedBytes()

    HandshakeInit(
      protocol,
      new String(bytesVersion, Charset(charset)),
      threadId,
      Array.concat(salt1, salt2),
      serverCap,
      charset,
      status
    )
  }
}

case class HandshakeInit(
  protocol: Byte,
  version: String,
  threadId: Int,
  salt: Array[Byte],
  serverCap: Capability,
  charset: Short,
  status: Short
) extends Result

/**
 * Represents the OK Packet received from the server. It is sent
 * to indicate that a command has completed succesfully.
 * [[http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-OK_Packet]]
 */
object OK extends Decoder[OK] {
  def decode(packet: Packet) = {
    val br = MysqlBuf.reader(packet.body)
    br.skip(1)
    OK(
      br.readVariableLong(),
      br.readVariableLong(),
      br.readUnsignedShortLE(),
      br.readUnsignedShortLE(),
      new String(br.take(br.remaining))
    )
  }
}

case class OK(
  affectedRows: Long,
  insertId: Long,
  serverStatus: Int,
  warningCount: Int,
  message: String
) extends Result

/**
 * Represents the Error Packet received from the server and the data sent along with it.
 * [[http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-ERR_Packet]]
 */
object Error extends Decoder[Error] {
  def decode(packet: Packet) = {
    // start reading after flag byte
    val br = MysqlBuf.reader(packet.body)
    br.skip(1)
    val code = br.readShortLE()
    val state = new String(br.take(6))
    val msg = new String(br.take(br.remaining))
    Error(code, state, msg)
  }
}

case class Error(code: Short, sqlState: String, message: String) extends Result

/**
 * Represents and EOF result received from the server which
 * contains any warnings and the server status.
 * [[http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet]]
 */
object EOF extends Decoder[EOF] {
  def decode(packet: Packet) = {
    val br = MysqlBuf.reader(packet.body)
    br.skip(1)
    EOF(br.readShortLE(), ServerStatus(br.readShortLE()))
  }
}

case class EOF(warnings: Short, serverStatus: ServerStatus) extends Result

/**
 * Represents the column meta-data associated with a query.
 * Sent during ResultSet transmission and as part of the
 * meta-data associated with a Row.
 * [[http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41]]
 */
object Field extends Decoder[Field] {
  def decode(packet: Packet): Field = {
    val bw = MysqlBuf.reader(packet.body)
    val bytesCatalog = bw.readLengthCodedBytes()
    val bytesDb = bw.readLengthCodedBytes()
    val bytesTable = bw.readLengthCodedBytes()
    val bytesOrigTable = bw.readLengthCodedBytes()
    val bytesName = bw.readLengthCodedBytes()
    val bytesOrigName = bw.readLengthCodedBytes()
    bw.readVariableLong() // length of the following fields (always 0x0c)
    val charset = bw.readShortLE()
    val jCharset = Charset(charset)
    val catalog = new String(bytesCatalog, jCharset)
    val db = new String(bytesDb, jCharset)
    val table = new String(bytesTable, jCharset)
    val origTable = new String(bytesOrigTable, jCharset)
    val name = new String(bytesName, jCharset)
    val origName = new String(bytesOrigName, jCharset)
    val length = bw.readIntLE()
    val fieldType = bw.readUnsignedByte()
    val flags = bw.readShortLE()
    val decimals = bw.readByte()
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
  decimals: Byte
) extends Result {
  def id: String = if (name.isEmpty) origName else name
  override val toString = "Field(%s)".format(id)
}

/**
 * Meta data returned from the server in response to
 * a prepared statement initialization request
 * COM_STMT_PREPARE.
 * [[http://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html#packet-COM_STMT_PREPARE_OK]]
 */
object PrepareOK extends Decoder[PrepareOK] {
  def decode(header: Packet) = {
    val br = MysqlBuf.reader(header.body)
    br.skip(1)
    val stmtId = br.readIntLE()
    val numCols = br.readUnsignedShortLE()
    val numParams = br.readUnsignedShortLE()
    br.skip(1)
    val warningCount = br.readUnsignedShortLE()
    PrepareOK(stmtId, numCols, numParams, warningCount)
  }
}

case class PrepareOK(
  id: Int,
  numOfCols: Int,
  numOfParams: Int,
  warningCount: Int,
  columns: Seq[Field] = Nil,
  params: Seq[Field] = Nil
) extends Result

/**
 * Used internally to synthesize a response from
 * the server when sending a prepared statement
 * CloseRequest
 */
object CloseStatementOK extends OK(0,0,0,0, "Internal Close OK")

/**
 * Resultset returned from the server containing field definitions and
 * rows. The rows can be binary encoded (for prepared statements)
 * or text encoded (for regular queries).
 * [[http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset]]
 * [[http://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html]]
 */
object ResultSet {
  def apply(isBinaryEncoded: Boolean)(
    header: Packet,
    fieldPackets: Seq[Packet],
    rowPackets: Seq[Packet]
  ): Try[ResultSet] = Try(decode(isBinaryEncoded)(header, fieldPackets, rowPackets))

  def decode(isBinaryEncoded: Boolean)(header: Packet, fieldPackets: Seq[Packet], rowPackets: Seq[Packet]): ResultSet = {
    val fields = fieldPackets.map(Field.decode).toIndexedSeq

    decodeRows(isBinaryEncoded, rowPackets, fields)
  }

  def decodeRows(isBinaryEncoded: Boolean, rowPackets: Seq[Packet], fields: IndexedSeq[Field]): ResultSet = {
    // A name -> index map used to allow quick lookups for rows based on name.
    val indexMap = fields.map(_.id).zipWithIndex.toMap

    /**
     * Rows can be encoded as Strings or Binary depending
     * on if the ResultSet is created by a normal query or
     * a prepared statement, respectively.
     */
    val rows = rowPackets.map { p: Packet =>
      if (!isBinaryEncoded)
        new StringEncodedRow(p.body, fields, indexMap)
      else
        new BinaryEncodedRow(p.body, fields, indexMap)
    }

    ResultSet(fields, rows)
  }
}

case class ResultSet(fields: Seq[Field], rows: Seq[Row]) extends Result {
  override def toString = s"ResultSet(${fields.size}, ${rows.size})"
}

object FetchResult {
  def apply(
    rowPackets: Seq[Packet],
    eofPacket: EOF
  ): Try[FetchResult] = {
    Try {
      val containsLastRow: Boolean = eofPacket.serverStatus.has(ServerStatus.LastRowSent)
      FetchResult(rowPackets, containsLastRow)
    }
  }

  def apply(
    err: Error
  ): Try[FetchResult] = {
    Return(FetchResult(Seq(), containsLastRow = true))
  }
}

case class FetchResult(rowPackets: Seq[Packet], containsLastRow: Boolean) extends Result {
  override def toString: String = s"FetchResult(rows=${rowPackets.size}, containsLastRow=$containsLastRow)"
}