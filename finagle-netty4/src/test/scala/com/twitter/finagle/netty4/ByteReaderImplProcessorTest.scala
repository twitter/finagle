package com.twitter.finagle.netty4

import com.twitter.io.{Buf, ByteReader}

class ByteReaderImplProcessorTest
    extends ReadableBufProcessorTest(
      "ByteReaderImpl",
      { bytes: Array[Byte] =>
        val br = ByteReader(Buf.ByteArray.Owned(bytes))
        new ReadableBufProcessorTest.CanProcess {
          def process(from: Int, until: Int, processor: Buf.Processor): Int =
            br.process(from, until, processor)
          def process(processor: Buf.Processor): Int = br.process(processor)
          def readBytes(num: Int): Unit = br.readBytes(num)
          def readerIndex(): Int = bytes.length - br.remaining
        }
      }
    )
