package com.twitter.finagle.mysql.util

import org.specs.SpecificationWithJUnit

class ByteArrayUtilSpec extends SpecificationWithJUnit {
  "ByteArrayUtilSpec" should {
    "Read Bytes" in {
      val bytes = Array[Byte](0x00, 0x00, 0x11, 0x22, 0x33, 0x44, 0x00)
      val result = ByteArrayUtil.read(bytes, 2, 4)
      result mustEqual 0x44332211
    }

    "Write Bytes" in {
      val number = 0x55213448
      val dst = new Array[Byte](4)
      ByteArrayUtil.write(number, dst, 0, 4)
      //make sure bytes were written in little endian
      dst(0) mustEqual 0x48
      dst(1) mustEqual 0x34
      dst(2) mustEqual 0x21
      dst(3) mustEqual 0x55
    }

    "Read Bytes after Write" in {
      val number = 0x55336712
      val dst = new Array[Byte](4)
      ByteArrayUtil.write(number, dst, 0, 4)
      number mustEqual ByteArrayUtil.read(dst, 0 , 4)
    }

    "Read signed int value" in {
      val n = 0xfffff6ff
      val dst = new Array[Byte](4)
      ByteArrayUtil.write(n, dst, 0, 4)
      n mustEqual ByteArrayUtil.read(dst, 0, 4)
    }

    "Write Null Terminated String" in {
      val str = "test"
      val dst = new Array[Byte](str.length+1)
      ByteArrayUtil.writeNullTerminatedString(str, dst, 0)
      dst must containAll(Array.concat(str.getBytes, Array('\0'.toByte)))
    }

    "Read Null Terminated String" in {
      val str = "Null Terminated String\0"
      str.take(str.size-1) mustEqual ByteArrayUtil.readNullTerminatedString(str.getBytes, 0)
    }

    "Write Length Encoded String" in {
      val str = "test"
      val dst = new Array[Byte](str.length+1)
      ByteArrayUtil.writeLengthCodedString(str, dst, 0)
      dst must containAll(Array.concat(Array(str.length.toByte), str.getBytes))
    }

    "Read Length Encoded String" in {
      val str = "Length Encoded String"
      val data = Array.concat(new Array[Byte](1), str.getBytes)
      data(0) = str.length.toByte
      str mustEqual ByteArrayUtil.readLengthCodedString(data, 0)
    }

    "Require valid width on read" in {
      ByteArrayUtil.read(new Array[Byte](9), 0, 9) must throwA[IllegalArgumentException]
    }

    "Require valid args in write" in {
      ByteArrayUtil.write(0xff, new Array[Byte](2), 0, 4) must throwA[IllegalArgumentException]
    }
  }
}