package com.twitter.finagle.exp.mysql

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.twitter.finagle.exp.mysql.transport.{Buffer, BufferReader}

@RunWith(classOf[JUnitRunner])
class RequestTest extends FunSuite {
  test("encode simple command request") {
    val bytes = "table".getBytes
    val cmd = 0x00
    val req = new SimpleCommandRequest(cmd.toByte, bytes)
    val buf = Buffer.fromChannelBuffer(req.toPacket.toChannelBuffer)
    val br = BufferReader(buf)
    assert(br.readInt24() === bytes.size + 1) // cmd byte
    assert(br.readByte() === 0x00)
    assert(br.readByte() === cmd)
    assert(br.take(bytes.size) === bytes)
  }
}
