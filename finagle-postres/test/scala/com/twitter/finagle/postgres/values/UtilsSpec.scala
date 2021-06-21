package com.twitter.finagle.postgres.values

import com.twitter.finagle.postgres.Spec
import io.netty.buffer.{ByteBuf, Unpooled}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import com.twitter.finagle.postgres.Generators._

class UtilsSpec extends Spec with ScalaCheckDrivenPropertyChecks {
  "Buffers.readCString" should {
    def newBuffer(): (ByteBuf, String, String) = {
      val str = "Some string"
      val cStr = str + '\u0000'
      val buffer = Unpooled.copiedBuffer(cStr, Charsets.Utf8)

      (buffer, str, cStr)
    }

    "fully read a string" in {
      val (buffer, str, cStr) = newBuffer()

      val actualStr = Buffers.readCString(buffer)
      actualStr must equal(str)
    }

    "set reader index to the right value after reading" in {
      val (buffer, str, cStr) = newBuffer()

      Buffers.readCString(buffer)

      buffer.readerIndex() must equal(cStr.length)
    }

    "respect the initial reader index" in {
      val (buffer, str, cStr) = newBuffer()
      buffer.readChar()

      Buffers.readCString(buffer)

      buffer.readerIndex() must equal(cStr.length)
    }

    "throw an appropriate exception if string passed is not C style" in {
      val bufferWithWrongString = Unpooled.copiedBuffer("not a C style string", Charsets.Utf8)

      an[IndexOutOfBoundsException] must be thrownBy {
        Buffers.readCString(bufferWithWrongString)
      }
    }
  }

  "Md5Encryptor.encrypt" should {
    def ba(str: String) = str.getBytes()

    "encrypt everything correctly" in {
      val samples =
        List(
          (ba("john"), ba("john25"), Array[Byte](1, 2, 3, 4), "md5305d62541687fa0c5871edfdb1140133"),
          (ba("john"), ba("john25"), Array[Byte](4, 3, 1, 2), "md5156cf720128cad07c39e018eca91ff8d"),
          (ba("john"), ba("john22"), Array[Byte](1, 2, 3, 4), "md57042902d6531e1840b3019a880f66edc"),
          (ba("lomack"), ba("lowmuck245$3"), Array[Byte](15, 19, 33, 1), "md56aa29016af76de6b793f3e7e009a26c2")
        )

      samples.foreach {
        case (user, password, salt, result) =>
          new String(Md5Encryptor.encrypt(user, password, salt)) must equal(result)
      }
    }

    "throw an exception if any of the parameters is missing or empty" in {
      val user = ba("john")
      val password = ba("john25")
      val salt = Array[Byte](1, 2, 3, 4)
      val empty = Array[Byte]()

      an[IllegalArgumentException] must be thrownBy {
        Md5Encryptor.encrypt(empty, password, salt)
      }

      an[IllegalArgumentException] must be thrownBy {
        Md5Encryptor.encrypt(null, password, salt)
      }

      an[IllegalArgumentException] must be thrownBy {
        Md5Encryptor.encrypt(user, empty, salt)
      }

      an[IllegalArgumentException] must be thrownBy {
        Md5Encryptor.encrypt(user, null, salt)
      }

      an[IllegalArgumentException] must be thrownBy {
        Md5Encryptor.encrypt(user, password, empty)
      }

      an[IllegalArgumentException] must be thrownBy {
        Md5Encryptor.encrypt(user, password, null)
      }
    }
  }

  "Numeric utils" should {
    "write a numeric value" in forAll {
      bd: BigDecimal =>
        val read = Numerics.readNumeric(Numerics.writeNumeric(bd))
        read must equal (bd)
    }
  }
}
