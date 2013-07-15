package com.twitter.finagle.exp.mysql.codec

import com.twitter.finagle.exp.mysql._
import com.twitter.finagle.exp.mysql.protocol._
import com.twitter.finagle.exp.mysql.{Error => ServerError}
import org.jboss.netty.buffer.ChannelBuffers._
import org.jboss.netty.channel._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndecTest extends FunSuite with BeforeAndAfterAll {
  val endec = new Endec
  val greetData = Array[Byte](
    10,53,46,53,46,50,52,0,31,0,0,0,70,38,43,66,74,
    48,79,126,0,-1,-9,33,2,0,15,-128,21,0,0,0,0,0,
    0,0,0,0,0,76,66,70,118,67,40,63,68,120,80,103,54,0
  )
  val greetPacket = Packet(52, 0, greetData)

  override def beforeAll() {
    // Handshake
    endec.encode(ClientInternalGreet, None)
    endec.decode(greetPacket)
  }

  test("Handshaking") {
    val e = new Endec
    e.encode(ClientInternalGreet, None)
    val sg = ServersGreeting.decode(greetPacket)
    val res = e.decode(greetPacket).get
    assert(res.isInstanceOf[ServersGreeting])
    val r = res.asInstanceOf[ServersGreeting]
    assert(r.protocol === sg.protocol)
    assert(r.version === sg.version)
    assert(r.threadId === sg.threadId)
    assert(r.serverCap == sg.serverCap)
    assert(r.charset == sg.charset)
    assert(r.status == sg.status)
  }

  test("decode OK packet") {
    // build ok packet
    val message = "Records: 3 Duplicates: 0 Warnings: 0"
    val size = 7 + message.size
    val okpacket = Packet(size, 0, new Array[Byte](size))
    val bw = BufferWriter(okpacket.body)
    bw.writeByte(0x00) // field count
    bw.writeLengthCodedBinary(0x01) // affected rows
    bw.writeLengthCodedBinary(0x00) // insert id
    bw.writeShort(0x0002) // server status
    bw.writeShort(0x0000) // warning count
    bw.writeBytes(message.getBytes(Charset.defaultCharset.displayName)) // message

    val expectedOK = OK.decode(okpacket)

    val resOpt = endec.decode(okpacket)
    assert(resOpt.isEmpty == false)
    assert(resOpt.get.isInstanceOf[OK])
    val res = resOpt.get.asInstanceOf[OK]
    assert(expectedOK === res)
  }

  test("decode Error Packet") {
    val message = "Unknown table 'q'"
    val size = 9 + message.size
    val errpacket = Packet(size, 1, new Array[Byte](size))
    val bw = BufferWriter(errpacket.body)
    bw.writeByte(0xff) // field count
    bw.writeShort(0x041b) //err no
    bw.writeBytes("#42S02".getBytes) // sqlstate
    bw.writeBytes(message.getBytes(Charset.defaultCharset.displayName))

    val expectedError = ServerError.decode(errpacket)

    val resOpt = endec.decode(errpacket)
    assert(resOpt.isEmpty == false)
    assert(resOpt.get.isInstanceOf[ServerError])
    val res = resOpt.get.asInstanceOf[ServerError]
    assert(expectedError === res)
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
          Type.VARCHAR,
          0,
          0
        )
        f :: aux(len-1)
    }
    aux(numFields)
  }

  def toPacket(f: Field): Packet = {
    def strLen(s: String) = Buffer.sizeOfLen(s.length) + s.length

    val sizeOfField = (strLen(f.catalog) + strLen(f.db)
      + strLen(f.table) + strLen(f.origTable)
      + strLen(f.name) + strLen(f.origName) + 12)

    val fieldData = new Array[Byte](sizeOfField)
    val bw = BufferWriter(fieldData)
    bw.writeLengthCodedString(f.catalog)
    bw.writeLengthCodedString(f.db)
    bw.writeLengthCodedString(f.table)
    bw.writeLengthCodedString(f.origTable)
    bw.writeLengthCodedString(f.name)
    bw.writeLengthCodedString(f.origName)
    bw.writeByte(0)
    bw.writeShort(f.charset)
    bw.writeInt(f.displayLength)
    bw.writeByte(f.fieldType)
    bw.writeShort(f.flags)
    bw.writeByte(f.decimals)
    Packet(sizeOfField, 0, fieldData)
  }

  val numFields = 5
  val numRows = 3
  val headerPacket = Packet(1, 0, Array(numFields.toByte))
  val eof = Packet(1, 0, Array(Packet.EofByte))

  // fields + field packets
  val fields = createFields(numFields)
  val fieldPackets = fields map { toPacket(_) }

  def rowPacket: Packet = {
    val valueSize = 7
    val bufferSize = numFields * valueSize
    val bw = BufferWriter(new Array[Byte](bufferSize))
    for (i <- 1 to numFields) {
      bw.writeLengthCodedString("value"+i)
    }

    Packet(valueSize, 0, bw.array)
  }

  val rowPackets = for (i <- 1 to numRows) yield rowPacket

  test("Get an empty result after header") {
    val headerRes = endec.decode(headerPacket)
    assert(headerRes.isEmpty)
  }

  test("Get empty results after field packets") {
    val fieldResults = fieldPackets map { endec.decode(_) }
    fieldResults foreach { res => assert(res.isEmpty) }
  }

  test("Get empty result after EOF #1") {
    val eofRes = endec.decode(eof)
    assert(eofRes.isEmpty)
  }

  test("Get empty result after rows") {
    val rowResults = rowPackets map { endec.decode(_) }
    rowResults foreach { res => assert(res.isEmpty) }
  }

  test("Get ResultSet after EOF #2") {
    val res = endec.decode(eof)
    assert(!res.isEmpty)
    assert(res.get.isInstanceOf[ResultSet])

    // Contain correct fields
    val rs = res.get.asInstanceOf[ResultSet]
    (0 until numFields) foreach { idx =>
      assert(fields(idx) === rs.fields(idx))
    }
  }

  def makePreparedHeader(numOfParams: Int, numColumns: Int) = {
    val bw = BufferWriter(new Array[Byte](12))
    bw.writeByte(0x00)
    bw.writeInt(1)
    bw.writeShort(numColumns)
    bw.writeShort(numOfParams)
    bw.writeByte(0x00)
    bw.writeShort(0x00)
    Packet(12, 0, bw.array)
  }

  test("decode PreparedStatement (numParams = 0, numCols = 0)") {
    // mock a prepared statement request
    endec.encode(PrepareRequest(""), None)
    val hdr = makePreparedHeader(0,0)
    val res = endec.decode(hdr)
    assert(!res.isEmpty)
    assert(res.get.isInstanceOf[PreparedStatement])
  }

  test("decode PreparedStatement (numParams = 0, numCols > 0)") {
    endec.encode(PrepareRequest(""), None)
    val hdr = makePreparedHeader(0, 1)
    val hdrRes = endec.decode(hdr)
    assert(hdrRes.isEmpty)

    val colRes = endec.decode(fieldPackets(0))
    assert(colRes.isEmpty)

    val res = endec.decode(eof)
    assert(!res.isEmpty)
    assert(res.get.isInstanceOf[PreparedStatement])
  }

  test("decode PreparedStatement (numParams > 0, numCols = 0)") {
    endec.encode(PrepareRequest(""), None)
    val hdr = makePreparedHeader(1, 0)
    val hdrRes = endec.decode(hdr)
    assert(hdrRes.isEmpty)

    val paramRes = endec.decode(fieldPackets(0))
    assert(paramRes.isEmpty)

    val res = endec.decode(eof)
    assert(!res.isEmpty)
    assert(res.get.isInstanceOf[PreparedStatement])
  }

  test("decode PreparedStatement (numParams > 0, numCols > 0)") {
    endec.encode(PrepareRequest(""), None)
    val hdr = makePreparedHeader(1, 1)
    val hdrRes = endec.decode(hdr)
    assert(hdrRes.isEmpty)

    val colRes = endec.decode(fieldPackets(0))
    assert(colRes.isEmpty)

    // eof 1
    val eofRes = endec.decode(eof)
    assert(eofRes.isEmpty)

    // Get an empty result after param packet(s)
    val paramRes = endec.decode(fieldPackets(0))
    assert(paramRes.isEmpty)

    // Get a PreparedStatement after EOF #2
    val res = endec.decode(eof)
    assert(!res.isEmpty)
    assert(res.get.isInstanceOf[PreparedStatement])
  }
}