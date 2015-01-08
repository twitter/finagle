package com.twitter.finagle.postgres.protocol

import com.twitter.finagle.postgres.values.{Md5Encryptor, Buffers, Charsets}
import org.specs2.mutable.Specification
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.buffer.ChannelBuffer
import org.specs2.matcher.DataTables

@RunWith(classOf[JUnitRunner])
class UtilsSpec extends Specification with DataTables {

  "Buffers readCString" should {

    def newBuffer(): (ChannelBuffer, String, String) = {
      val str = "Some string"
      val cStr = str + '\0'
      val buffer = ChannelBuffers.copiedBuffer(cStr, Charsets.Utf8)
      (buffer, str, cStr)
    }

    "fully read a string" in {
      val (buffer, str, cStr) = newBuffer

      val actualStr = Buffers.readCString(buffer)
      actualStr === str
    }

    "set reader index to right value after reading" in {
      val (buffer, str, cStr) = newBuffer

      Buffers.readCString(buffer)

      buffer.readerIndex() === cStr.length
    }

    "respect initial reader index" in {
      val (buffer, str, cStr) = newBuffer
      buffer.readChar()

      Buffers.readCString(buffer)

      buffer.readerIndex() === cStr.length
    }

    "throw appropriate exception if string passed is not C style" in {
      val bufferWithWrongString = ChannelBuffers.copiedBuffer("not a C style string", Charsets.Utf8)

      Buffers.readCString(bufferWithWrongString) must throwAn[IndexOutOfBoundsException]
    }
  }

  "Md5Encryptor encrypt" should {

    def ba(str: String) = str.getBytes()

    "encrypt everything right" ! samples

    def samples =
      "user" | "password" | "salt" | "result" |
        ba("john") ! ba("john25") ! Array[Byte](1, 2, 3, 4) ! "md5305d62541687fa0c5871edfdb1140133" |
        ba("john") ! ba("john25") ! Array[Byte](4, 3, 1, 2) ! "md5156cf720128cad07c39e018eca91ff8d" |
        ba("john") ! ba("john22") ! Array[Byte](1, 2, 3, 4) ! "md57042902d6531e1840b3019a880f66edc" |
        ba("lomack") ! ba("lowmuck245$3") ! Array[Byte](15, 19, 33, 1) ! "md56aa29016af76de6b793f3e7e009a26c2" |> {
        (user, password, salt, result) => new String(Md5Encryptor.encrypt(user, password, salt)) must_== result
      }

    "throw exception if any of the parameters is missing or empty" in {
      val user = ba("john")
      val password = ba("john25")
      val salt = Array[Byte](1, 2, 3, 4)
      val empty = Array[Byte]()

      Md5Encryptor.encrypt(empty, password, salt) must throwAn[IllegalArgumentException]
      Md5Encryptor.encrypt(null, password, salt) must throwAn[IllegalArgumentException]
      Md5Encryptor.encrypt(user, empty, salt) must throwAn[IllegalArgumentException]
      Md5Encryptor.encrypt(user, null, salt) must throwAn[IllegalArgumentException]
      Md5Encryptor.encrypt(user, password, empty) must throwAn[IllegalArgumentException]
      Md5Encryptor.encrypt(user, password, null) must throwAn[IllegalArgumentException]
    }
  }
}