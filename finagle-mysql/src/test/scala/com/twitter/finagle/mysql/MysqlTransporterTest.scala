package com.twitter.finagle.mysql

import com.twitter.finagle.{Stack, Status}
import com.twitter.finagle.mysql.transport.Packet
import com.twitter.util.{Await, Duration}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, SocketAddress}
import org.scalatest.FunSuite

class MysqlTransporterTest extends FunSuite {
  // This is an example MySQL server response in bytes
  val serverBytes: Array[Byte] = Array(74, 0, 0, 0, 10, 53, 46, 55, 46, 50, 49, 0, -110, 14, 0, 0,
    29, 65, 18, 114, 89, 41, 104, 101, 0, -1, -9, 33, 2, 0, -1, -127, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 37, 39, 12, 74, 93, 61, 77, 119, 52, 29, 4, 101, 0, 109, 121, 115, 113, 108, 95, 110, 97,
    116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0)

  test("MysqlTransporter remoteAddress is the address passed in") {
    val addr: SocketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val transporter = new MysqlTransporter(addr, Stack.Params.empty)
    assert(transporter.remoteAddress == addr)
  }

  test("MysqlTransporter can create a transport for MySQL") {
    // Setup the ServerSocket. 50 is the default for the listen backlog.
    // Need to supply it in order to supply the third param (bindAddr)
    val server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    try {
      val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort)
      val transporter = new MysqlTransporter(addr, Stack.Params.empty)
      val transportFut = transporter()
      val acceptedSocket = server.accept()
      val transport = Await.result(transportFut, Duration.fromSeconds(2))
      try {
        assert(transport.status == Status.Open)

        // Write the MySQL initial greeting to the client
        val outStream = acceptedSocket.getOutputStream
        outStream.write(serverBytes)
        outStream.flush()
        outStream.close()

        // Read the initial greeting on the client side
        // Make sure that it can be seen as a MySQL Packet
        val response = Await.result(transport.read(), Duration.fromSeconds(2))
        val packet = Packet.fromBuf(response)
        assert(packet.seq == 0)
        assert(packet.body.length == 74)
      } finally {
        transport.close()
      }
    } finally {
      server.close()
    }
  }
}
