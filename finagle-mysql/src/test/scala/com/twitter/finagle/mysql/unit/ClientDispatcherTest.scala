package com.twitter.finagle.mysql

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.mysql.transport.{Packet, MysqlBuf}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.Await
import com.twitter.io.Buf
import org.scalatest.FunSuite

class ClientDispatcherTest extends FunSuite {
  val rawInit = Array[Byte](
    10, 53, 46, 53, 46, 50, 52, 0, 31, 0, 0, 0, 70, 38, 43, 66, 74, 48, 79, 126, 0, -1, -9, 33, 2,
    0, 15, -128, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 76, 66, 70, 118, 67, 40, 63, 68, 120, 80, 103,
    54, 0
  )
  val initPacket = Packet(0, Buf.ByteArray.Owned(rawInit))
  val init = HandshakeInit.decode(initPacket)

  val handshake = Handshake(Some("username"), Some("password"))
  val initReply = handshake(init)

  def newCtx = new {
    val clientq = new AsyncQueue[Packet]()
    val serverq = new AsyncQueue[Packet]()
    val trans = new QueueTransport[Packet, Packet](serverq, clientq)
    val service = new ClientDispatcher(trans, handshake, supportUnsigned = false)
    // authenticate
    clientq.offer(initPacket)
    val handshakeResponse = serverq.poll()
    clientq.offer(okPacket)
  }

  val okPacket =
    Packet(1, Buf.ByteArray.Owned(Array[Byte](0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00)))

  test("handshaking") {
    val ctx = newCtx
    import ctx._
    val packet = Await.result(handshakeResponse)
    val br = MysqlBuf.reader(packet.body)
    assert(br.readIntLE() == initReply().clientCap.mask)
    assert(br.readIntLE() == initReply().maxPacketSize)
    assert(br.readByte() == initReply().charset)
    assert(br.take(23) === Array.fill(23)(0.toByte))
    assert(br.readNullTerminatedString() == "username")
  }

  test("serially dispatch requests") {
    val ctx = newCtx
    import ctx._
    val r1 = service(QueryRequest("SELECT 1"))
    val r2 = service(QueryRequest("SELECT 2"))
    val r3 = service(QueryRequest("SELECT 3"))
    assert(serverq.size == 1)
    clientq.offer(okPacket)
    assert(r1.isDefined)
    assert(!r2.isDefined)
    assert(!r3.isDefined)
  }

  test("decode OK packet") {
    val ctx = newCtx
    import ctx._
    val r = service(PingRequest)
    clientq.offer(okPacket)
    assert(Await.result(r).isInstanceOf[OK])
    val okResult = Await.result(r).asInstanceOf[OK]
    assert(okResult == OK.decode(okPacket))
  }

  test("decode Error packet as ServerError") {
    val ctx = newCtx
    import ctx._
    val message = "Unknown table 'q'"
    val size = 9 + message.size
    val bw = MysqlBuf.writer(new Array[Byte](size))
    bw.writeByte(0xff) // field count
    bw.writeShortLE(0x041b) //err no
    bw.writeBytes("#42S02".getBytes) // sqlstate
    bw.writeBytes(message.getBytes(Charset.defaultCharset.displayName))

    val errpacket = Packet(1, bw.owned())
    val expectedError = Error.decode(errpacket)

    val r = service(QueryRequest("SELECT * FROM q"))
    clientq.offer(errpacket)
    intercept[ServerError] {
      Await.result(r)
    }
  }

  def createFields(numFields: Int): List[Field] = {
    val catalog = "def"
    val db = "database0"
    val table = "table0"

    def aux(len: Int): List[Field] = len match {
      case 0 => Nil
      case x =>
        val maxLen = 12
        val fieldName = "field" + len
        val f = Field(
          catalog,
          db,
          table,
          table,
          fieldName,
          fieldName,
          33.toShort,
          maxLen,
          Type.VarChar,
          0,
          0
        )
        f :: aux(len - 1)
    }
    aux(numFields)
  }

  def toPacket(f: Field): Packet = {
    def strLen(s: String) = MysqlBuf.sizeOfLen(s.length) + s.length

    val sizeOfField = (strLen(f.catalog) + strLen(f.db)
      + strLen(f.table) + strLen(f.origTable)
      + strLen(f.name) + strLen(f.origName) + 12)

    val bw = MysqlBuf.writer(new Array[Byte](sizeOfField))

    def writeString(s: String) = bw.writeLengthCodedString(s, Charset.defaultCharset)

    writeString(f.catalog)
    writeString(f.db)
    writeString(f.table)
    writeString(f.origTable)
    writeString(f.name)
    writeString(f.origName)
    bw.writeByte(0)
    bw.writeShortLE(f.charset)
    bw.writeIntLE(f.displayLength)
    bw.writeByte(f.fieldType)
    bw.writeShortLE(f.flags)
    bw.writeByte(f.decimals)
    Packet(0, bw.owned())
  }

  val numFields = 5
  val numRows = 3
  val headerPacket = Packet(0, Buf.ByteArray.Owned(Array(numFields.toByte)))
  val eof = Packet(0, Buf.ByteArray.Owned(Array[Byte](Packet.EofByte, 0x00, 0x00, 0x00, 0x00)))
  val fields = createFields(numFields)
  val fieldPackets = fields.map(toPacket)

  def rowPacket: Packet = {
    val valueSize = 7
    val bufferSize = numFields * valueSize
    val bw = MysqlBuf.writer(new Array[Byte](bufferSize))
    for (i <- 1 to numFields) {
      bw.writeLengthCodedString("value" + i, Charset.defaultCharset)
    }

    Packet(0, bw.owned())
  }

  val rowPackets = for (i <- 1 to numRows) yield rowPacket

  test("Decode a ResultSet") {
    val ctx = newCtx
    import ctx._
    val query = service(QueryRequest("SELECT 1 + 1"))
    clientq.offer(headerPacket)
    fieldPackets foreach { clientq.offer(_) }
    clientq.offer(eof)
    rowPackets foreach { clientq.offer(_) }
    clientq.offer(eof)
    assert(Await.result(query).isInstanceOf[ResultSet])
    val rs = Await.result(query).asInstanceOf[ResultSet]
    assert(rs.fields.size == numFields)
    assert(rs.rows.size == numRows)
  }

  def makePreparedHeader(numColumns: Int, numParams: Int) = {
    val bw = MysqlBuf.writer(new Array[Byte](12))
    bw.writeByte(0x00) // ok byte
    bw.writeIntLE(1) // stmt id
    bw.writeShortLE(numColumns)
    bw.writeShortLE(numParams)
    bw.writeByte(0x00) // reserved byte
    bw.writeShortLE(0x00) // warning count
    Packet(0, bw.owned())
  }

  test("Decode PreparedStatement numParams = 0, numCols = 0") {
    val ctx = newCtx
    import ctx._
    val query = service(PrepareRequest(""))
    clientq.offer(makePreparedHeader(0, 0))
    assert(Await.result(query).isInstanceOf[PrepareOK])
    val res = Await.result(query).asInstanceOf[PrepareOK]
    assert(res.numOfCols == 0)
    assert(res.numOfParams == 0)
  }

  test("Decode PreparedStatement numParams > 0, numCols > 0") {
    val ctx = newCtx
    import ctx._
    val query = service(PrepareRequest("SELECT name FROM t1 WHERE id IN (?, ?, ?, ?, ?)"))
    val numParams = numFields
    clientq.offer(makePreparedHeader(1, numParams))
    fieldPackets foreach { clientq.offer(_) }
    clientq.offer(eof)
    val cols = createFields(1)
    val colPackets = cols map { toPacket(_) }
    colPackets foreach { clientq.offer(_) }
    clientq.offer(eof)
    assert(Await.result(query).isInstanceOf[PrepareOK])
    val res = Await.result(query).asInstanceOf[PrepareOK]
    assert(res.numOfCols == 1)
    assert(res.numOfParams == numParams)
    assert(res.columns == cols.toList)
    assert(res.params == fields.toList)
  }

  test("CloseStatement satisfies rpc") {
    val ctx = newCtx
    import ctx._
    val stmtId = 5
    val query = service(CloseRequest(5))
    val sent = Await.result(serverq.poll())
    val br = MysqlBuf.reader(sent.body)
    assert(br.readByte() == Command.COM_STMT_CLOSE)
    assert(br.readIntLE() == stmtId)
    // response should be synthesized
    val resp = Await.result(query)
    assert(resp.isInstanceOf[OK])
  }

  test("LostSyncException closes the service") {
    val ctx = newCtx
    import ctx._
    // offer an ill-formed packet
    clientq.offer(Packet(0, Buf.ByteArray.Owned(Array[Byte]())))
    intercept[LostSyncException] { Await.result(service(PingRequest)) }
    assert(!service.isAvailable)
    assert(trans.onClose.isDefined)
  }

  test("Failure to auth closes the service") {
    val clientq = new AsyncQueue[Packet]()
    val trans = new QueueTransport[Packet, Packet](new AsyncQueue[Packet](), clientq)
    val service = new ClientDispatcher(trans, handshake, supportUnsigned = false)
    clientq.offer(Packet(0, Buf.ByteArray.Owned(Array[Byte]())))
    intercept[LostSyncException] { Await.result(service(PingRequest)) }
    assert(!service.isAvailable)
    assert(trans.onClose.isDefined)
  }
}
