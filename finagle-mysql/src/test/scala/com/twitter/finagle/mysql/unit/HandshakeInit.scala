package com.twitter.finagle.exp.mysql

import com.twitter.finagle.exp.mysql.transport.{Buffer, Packet}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class HandshakeInitTest extends FunSuite {
  val greeting = Array[Byte](
    10,53,46,53,46,50,52,0,31,0,0,0,70,38,43,66,74,
    48,79,126,0,-1,-9,33,2,0,15,-128,21,0,0,0,0,0,
    0,0,0,0,0,76,66,70,118,67,40,63,68,120,80,103,54,0
  )

  val packet = Packet(0, Buffer(greeting))
  val sg = HandshakeInit.decode(packet)

  test("protocol number") {
    assert(sg.protocol === 10)
  }

  test("version") {
    assert(sg.version === "5.5.24")
  }

  test("thread id") {
    assert(sg.threadId === 31)
  }

  test("salt") {
    assert(sg.salt.length === 20)
    val s = Array[Byte](70,38,43,66,74,48,79,126,76,66,
                        70,118,67,40,63,68,120,80,103,54)
    assert(sg.salt === s)
  }

  test("server capabilities") {
    assert(sg.serverCap.mask === 0xf7ff)
  }

  test("server collation code") {
    assert(sg.charset === 33)
  }

  test("server status") {
    assert(sg.status === 2)
  }
}
