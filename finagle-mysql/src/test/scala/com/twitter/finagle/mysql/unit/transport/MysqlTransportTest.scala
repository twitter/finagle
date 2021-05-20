package com.twitter.finagle.mysql.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.transport.QueueTransport
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration}
import org.scalatest.funsuite.AnyFunSuite

class MysqlTransportTest extends AnyFunSuite {
  // This is an example MySQL server response in bytes
  val serverBytes: Array[Byte] = Array(74, 0, 0, 0, 10, 53, 46, 55, 46, 50, 49, 0, -110, 14, 0, 0,
    29, 65, 18, 114, 89, 41, 104, 101, 0, -1, -9, 33, 2, 0, -1, -127, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 37, 39, 12, 74, 93, 61, 77, 119, 52, 29, 4, 101, 0, 109, 121, 115, 113, 108, 95, 110, 97,
    116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0)

  test("MysqlTransport understands reading MySQL Packets") {
    val readq = new AsyncQueue[Buf]()
    val writeq = new AsyncQueue[Buf]()

    val qtrans = new QueueTransport(writeq, readq)
    val trans = new MysqlTransport(qtrans.map(_.toBuf, Packet.fromBuf))
    readq.offer(Buf.ByteArray.Owned(serverBytes))

    // Read the initial greeting and ensure that it can be understood.
    val packet = Await.result(trans.read(), Duration.fromSeconds(1))
    assert(packet.seq == 0)
    assert(packet.body.length == 74)
  }

  test("MysqlTransport understands writing MySQL Packets") {
    // This test works by testing whether close properly writes
    // the QuitCommand.
    val readq = new AsyncQueue[Buf]()
    val writeq = new AsyncQueue[Buf]()

    val qtrans = new QueueTransport(writeq, readq)
    val trans = new MysqlTransport(qtrans.map(_.toBuf, Packet.fromBuf))
    trans.close()

    // https://dev.mysql.com/doc/internals/en/com-quit.html
    val quitBuf = Await.result(writeq.poll(), Duration.fromSeconds(1))
    assert(quitBuf.length == 5)
    assert(quitBuf.get(0) == 0x01)
    assert(quitBuf.get(1) == 0x00)
    assert(quitBuf.get(2) == 0x00)
    assert(quitBuf.get(3) == 0x00)
    assert(quitBuf.get(4) == 0x01)
  }

}
