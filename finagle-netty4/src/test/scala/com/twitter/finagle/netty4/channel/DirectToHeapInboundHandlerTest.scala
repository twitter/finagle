package com.twitter.finagle.netty4.channel

import io.netty.buffer.ByteBuf
import io.netty.channel.embedded.EmbeddedChannel
import org.junit.runner.RunWith
import org.scalacheck.Gen
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, OneInstancePerTest}
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class DirectToHeapInboundHandlerTest extends FunSuite
  with GeneratorDrivenPropertyChecks
  with OneInstancePerTest {

  val channel = new EmbeddedChannel(DirectToHeapInboundHandler)

  test("converts direct to heap") {
    forAll(Gen.alphaStr.suchThat(_.nonEmpty)) { s =>
      val in = channel.alloc.directBuffer(s.length)
      in.setBytes(0, s.getBytes("UTF-8"))

      channel.writeInbound(in)

      val out = channel.readInbound[ByteBuf]

      assert(!out.isDirect)
      assert(in == out)
    }
  }

  test("skips non-ByteBufs") {
    forAll { s: String =>
      channel.writeInbound(s)
      assert(channel.readInbound[String] == s)
    }
  }

  test("works when readIndex is not zero") {
    val in = channel.alloc.directBuffer(4)
    in.writeBytes(Array[Byte](0x1, 0x2, 0x3, 0x4))
    in.readerIndex(1)

    channel.writeInbound(in)

    val out = channel.readInbound[ByteBuf]
    assert(!out.isDirect)
    assert(out.readByte() == 0x2)
  }
}

