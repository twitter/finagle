package com.twitter.finagle.mysql

import com.twitter.finagle.Stack
import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.mysql.param.{Credentials, Database}
import com.twitter.finagle.mysql.transport.{MysqlTransport, Packet}
import com.twitter.finagle.transport.QueueTransport
import com.twitter.io.Buf
import com.twitter.util.{Await, Duration}
import org.scalatest.funsuite.AnyFunSuite

class PlainHandshakeTest extends AnyFunSuite {
  val initialBytes: Array[Byte] = Array(74, 0, 0, 0, 10, 53, 46, 55, 46, 50, 52, 0, -71, 44, 0, 0,
    88, 10, 77, 4, 94, 126, 122, 117, 0, -1, -1, 8, 2, 0, -1, -63, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    64, 116, 69, 9, 124, 24, 53, 73, 96, 24, 14, 21, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116,
    105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0)

  val handshakeResponseResult: Array[Byte] = Array(7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0)

  test("plain handshake works") {
    val readq = new AsyncQueue[Buf]()
    val writeq = new AsyncQueue[Buf]()

    val qtrans = new QueueTransport(writeq, readq)
    val trans = new MysqlTransport(qtrans.map(_.toBuf, Packet.fromBuf))
    readq.offer(Buf.ByteArray.Owned(initialBytes))
    readq.offer(Buf.ByteArray.Owned(handshakeResponseResult))

    val params =
      Stack.Params.empty + Credentials(Some("username"), Some("password")) + Database(Some("test"))
    val handshake = new PlainHandshake(params, trans)

    val result = Await.result(handshake.connectionPhase, Duration.fromSeconds(1))
    result match {
      case OK(rows, id, status, count, message) => assert(rows == 0)
      case other => fail(s"Unexpected result $other")
    }
  }
}
