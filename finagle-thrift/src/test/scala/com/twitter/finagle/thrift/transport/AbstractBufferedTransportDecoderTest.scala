package com.twitter.finagle.thrift.transport

import com.twitter.finagle.thrift.Protocols
import com.twitter.test.B.Client
import org.apache.thrift.transport.TMemoryBuffer
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractBufferedTransportDecoderTest extends AnyFunSuite {

  /**
   * Decode the sequence of input 'chunks' into a sequence of messages
   *
   * The input chunks may be fragmented in any manner, including empty
   * chunks. The output is expected to be fully formed messages
   */
  def decode(arrays: Seq[Array[Byte]]): Seq[Array[Byte]]

  // A message from the client
  private val messageArray: Array[Byte] = {
    val transport = new TMemoryBuffer(16)
    val prot = Protocols.factory().getProtocol(transport)
    val client = new Client(prot)
    client.send_multiply(1, 2)

    transport.getArray.take(transport.length())
  }

  test("Pass through a single message") {
    val messages = decode(Seq(messageArray))
    assert(messages.length == 1)
    assert(messages.head.sameElements(messageArray))
  }

  test("Slice multiple messages") {
    { // sent concatenated
      val results = decode(Seq(messageArray ++ messageArray))
      assert(results.length == 2)
      results.foreach(data => assert(data.sameElements(messageArray)))
    }

    { // sent discretely
      val results = decode(Seq(messageArray, messageArray))
      assert(results.length == 2)
      results.foreach(data => assert(data.sameElements(messageArray)))
    }
  }

  test("Slice a message from a following partial message") {
    val bonus = messageArray.take(4)
    val results = decode(Seq(messageArray ++ bonus))
    assert(results.length == 1)
    results.foreach(data => assert(data.sameElements(messageArray)))
  }

  test("Aggregate partial messages") {
    val results = decode(Seq(messageArray.take(4), messageArray.drop(4)))
    assert(results.length == 1)
    results.foreach(data => assert(data.sameElements(messageArray)))
  }

  test("Aggregate single byte input interspersed with empty input") {
    val results = decode(
      (messageArray ++ messageArray).toSeq
        .flatMap(byte => Seq(Array(byte), Array[Byte]()))
    ) // Seq(Array(1), Array(), Array(1)...)
    assert(results.length == 2)
    results.foreach(data => assert(data.sameElements(messageArray)))
  }
}
