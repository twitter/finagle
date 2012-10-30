package com.twitter.finagle.postgres.protocol

import org.jboss.netty.buffer.ChannelBuffers
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import java.util.Arrays

@RunWith(classOf[JUnitRunner])
class PacketSpec extends Specification {

  "Packet" should {
    "Encode empty packet" in {
      val pack = PacketBuilder().toPacket.encode

      pack.readInt === 4
    }
    "Encode one byte packet" in {
      val pack = PacketBuilder().writeByte(30).toPacket.encode

      pack.readInt === 5
      pack.readByte === 30
    }
    "Encode one char packet" in {
      val pack = PacketBuilder().writeChar('c').toPacket.encode

      pack.readInt === 5
      pack.readByte.asInstanceOf[Char] === 'c'
    }
    "Encode one short packet" in {
      val pack = PacketBuilder().writeShort(30).toPacket.encode

      pack.readInt === 6
      pack.readShort === 30
    }
    "Encode one int packet" in {
      val pack = PacketBuilder().writeInt(30).toPacket.encode

      pack.readInt === 8
      pack.readInt === 30
    }
    "Encode one string packet" in {
      val pack = PacketBuilder().writeCString("two").toPacket.encode

      pack.readInt === 8
    }
    "Encode named packe one int packet" in {
      val pack = PacketBuilder('c').writeInt(30).toPacket.encode

      pack.readByte.asInstanceOf[Char] === 'c'
      pack.readInt === 8
      pack.readInt === 30
    }
  }

}