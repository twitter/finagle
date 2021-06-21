package com.twitter.finagle.mysql

import com.twitter.finagle.{Stack, Status}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Await, Duration}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, SocketAddress}
import org.scalatest.funsuite.AnyFunSuite

class MysqlTransporterTest extends AnyFunSuite {

  // This is an example MySQL server response in bytes
  val initialBodyLength: Byte = 74
  val initialBytes: Array[Byte] = Array(74, 0, 0, 0, 10, 53, 46, 55, 46, 50, 52, 0, -71, 44, 0, 0,
    88, 10, 77, 4, 94, 126, 122, 117, 0, -1, -1, 8, 2, 0, -1, -63, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    64, 116, 69, 9, 124, 24, 53, 73, 96, 24, 14, 21, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116,
    105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0)

  val handshakeResponseResult: Array[Byte] = Array(7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0)

  test("MysqlTransporter remoteAddress is the address passed in") {
    val addr: SocketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val transporter = new MysqlTransporter(addr, Stack.Params.empty)
    assert(transporter.remoteAddress == addr)
  }

  test("MysqlTransporter can create a Transport which performs a plain handshake") {
    // Setup the ServerSocket. 50 is the default for the listen backlog.
    // Need to supply it in order to supply the third param (bindAddr)
    val server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    try {
      val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort)
      val transporter = new MysqlTransporter(addr, Stack.Params.empty)
      val transportFut = transporter()
      val acceptedSocket = server.accept()
      // Write the MySQL initial greeting to the client
      val outStream = acceptedSocket.getOutputStream
      outStream.write(initialBytes)
      outStream.flush()

      outStream.write(handshakeResponseResult)
      outStream.flush()

      val transport = Await.result(transportFut, Duration.fromSeconds(5))
      try {
        assert(transport.status == Status.Open)

        val result = Await.result(transport.close(), Duration.fromSeconds(5))
      } finally {
        transport.close()
      }
    } finally {
      server.close()
    }
  }

  test("MysqlTransporter doesn't try to connect initially using SSL/TLS") {
    // Setup the ServerSocket. 50 is the default for the listen backlog.
    // Need to supply it in order to supply the third param (bindAddr)
    val server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    try {
      val sslClientConfig = SslClientConfiguration()
      val params = Stack.Params.empty + Transport.ClientSsl(Some(sslClientConfig))

      // If the `ClientSsl` param is not properly removed when creating the underlying
      // Transporter, we will end up trying to negotiate SSL/TLS on connection
      // establishment, instead of waiting until the appropriate point in the MySQL
      // protocol handshake.

      val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, server.getLocalPort)
      val transporter = new MysqlTransporter(addr, params)
      val transportFut = transporter()
      val acceptedSocket = server.accept()

      // Write the MySQL initial greeting to the client
      val outStream = acceptedSocket.getOutputStream
      outStream.write(initialBytes)
      outStream.flush()

      val inBuffer = Array[Byte](0)
      val inStream = acceptedSocket.getInputStream
      val didRead = inStream.read(inBuffer, 0, 1)
      assert(didRead == 1)

      // We test that our finagle-mysql client is returning the
      // SSL Connection Request Packet. The packet body size of
      // the SslConnectionRequest is always 32 bytes. So if the
      // first byte received (the packet size) is 32, we consider
      // this successful.
      assert(inBuffer(0) == 32)

      outStream.close()
      inStream.close()
    } finally {
      server.close()
    }
  }

}
