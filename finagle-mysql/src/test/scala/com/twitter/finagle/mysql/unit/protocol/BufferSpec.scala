package com.twitter.finagle.mysql.util

import org.specs.SpecificationWithJUnit
import com.twitter.finagle.mysql.protocol.{BufferReader, BufferWriter}

class BufferSpec extends SpecificationWithJUnit {
  "Buffer" should {
    "read" in {
      val bytes = Array[Byte](0x11,0x22,0x33,0x44,0x55,0x66,0x77,0x78)
      val br = BufferReader(bytes)
      "Byte" in {
        br.readByte() mustEqual 0x11
        br.readByte() mustEqual 0x22
        br.readByte() mustEqual 0x33
        br.readByte() mustEqual 0x44
        br.readByte() mustEqual 0x55
        br.readByte() mustEqual 0x66
        br.readByte() mustEqual 0x77
        br.readByte() mustEqual 0x78
        br.readByte() must throwA[ArrayIndexOutOfBoundsException]
      }

      "Short" in {
        br.readShort() mustEqual 0x2211
        br.readShort() mustEqual 0x4433
        br.readShort() mustEqual 0x6655
        br.readShort() mustEqual 0x7877
        br.readByte() must throwA[ArrayIndexOutOfBoundsException]
      }

      "Int24" in {
        br.readInt24() mustEqual 0x332211
        br.readInt24() mustEqual 0x665544
        br.readShort() mustEqual 0x7877
        br.readByte() must throwA[ArrayIndexOutOfBoundsException]
      }

      "Int" in {
        br.readInt() mustEqual 0x44332211
        br.readInt() mustEqual 0x78776655
        br.readByte() must throwA[ArrayIndexOutOfBoundsException]
      }

      "Signed Int" in {
        val n = 0xfffff6ff
        val br = BufferReader(Array[Byte](0xff.toByte, 0xf6.toByte, 0xff.toByte, 0xff.toByte))
        n mustEqual br.readInt()
      }

      "Long" in {
        br.readLong() mustEqual 0x7877665544332211L
        br.readByte() must throwA[ArrayIndexOutOfBoundsException]
      }

      "Read length coded binary" in {
        1 mustEqual 1
      }

      "Read null terminated string" in {
        val str = "Null Terminated String\0"
        val br = BufferReader(str.getBytes)
        str.take(str.size-1) mustEqual br.readNullTerminatedString()
        br.readByte() must throwA[ArrayIndexOutOfBoundsException]
      }

      "Read length coded string" in {
        val str = "test"
        val bytes = Array.concat(Array.concat(Array(str.size.toByte), str.getBytes))
        val br = BufferReader(bytes)
        str mustEqual br.readLengthCodedString()
        br.readByte() must throwA[ArrayIndexOutOfBoundsException]
      }
    }

    "write" in {
      val bytes = new Array[Byte](8)
      val bw = BufferWriter(bytes)
      val br = BufferReader(bytes)
      "byte" in {
        bw.writeByte(0x01.toByte)
        0x01 mustEqual br.readByte()
      }

      "Short" in {
        bw.writeShort(0xFE.toShort)
        0xFE mustEqual br.readShort()
      }

      "Int24" in {
        bw.writeInt24(0x872312)
        0x872312 mustEqual br.readInt24()
      }

      "Int" in {
        bw.writeInt(0x98765432)
        0x98765432 mustEqual br.readInt()
      }

      "Long" in {
        bw.writeLong(0x7877665544332211L)
        0x7877665544332211L mustEqual br.readLong
        bw.writeByte(0x00.toByte) must throwA[ArrayIndexOutOfBoundsException]
      }

      "Write null terminated string" in {
        val str = "test\0"
        bw.writeNullTerminatedString(str)
        str.take(str.length-1) mustEqual br.readNullTerminatedString()
      }

      "Write length coded string" in {
        val str = "test"
        bw.writeLengthCodedString(str)
        str mustEqual br.readLengthCodedString()
      }
    }
  }
}