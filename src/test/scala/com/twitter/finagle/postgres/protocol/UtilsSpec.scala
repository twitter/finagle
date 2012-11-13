package com.twitter.finagle.postgres.protocol

import org.specs2.mutable.Specification
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.buffer.ChannelBuffer

@RunWith(classOf[JUnitRunner])
class UtilsSpec extends Specification {
  
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
}