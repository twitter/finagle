package com.twitter.finagle.postgres.messages

import com.twitter.finagle.postgres.Spec

class PacketSpec extends Spec {
  "Packet" should {
    "encode an empty packet" in {
      val pack = PacketBuilder().toPacket.encode

      pack.readInt must equal(4)
    }

    "encode a one byte packet" in {
      val pack = PacketBuilder().writeByte(30).toPacket.encode

      pack.readInt must equal(5)
      pack.readByte must equal(30)
    }

    "encode a one char packet" in {
      val pack = PacketBuilder().writeChar('c').toPacket.encode

      pack.readInt must equal(5)
      pack.readByte.asInstanceOf[Char] must equal('c')
    }

    "encode a one short packet" in {
      val pack = PacketBuilder().writeShort(30).toPacket.encode

      pack.readInt must equal(6)
      pack.readShort must equal(30)
    }

    "encode a one int packet" in {
      val pack = PacketBuilder().writeInt(30).toPacket.encode

      pack.readInt must equal(8)
      pack.readInt must equal(30)
    }

    "encode a one string packet" in {
      val pack = PacketBuilder().writeCString("two").toPacket.encode

      pack.readInt must equal(8)
    }

    "encode a packet with chars" in {
      val pack = PacketBuilder('c').writeInt(30).toPacket.encode

      pack.readByte.asInstanceOf[Char] must equal('c')
      pack.readInt must equal(8)
      pack.readInt must equal(30)
    }
  }
}