package com.twitter.finagle.netty4

import com.twitter.io.Buf
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalatest.funsuite.AnyFunSuite

object ReadableBufProcessorTest {

  trait CanProcess {
    def process(from: Int, until: Int, processor: Buf.Processor): Int
    def process(processor: Buf.Processor): Int
    def readBytes(num: Int): Unit
    def readerIndex(): Int
  }

  def newExceptionalProcessor(): Buf.Processor = new Buf.Processor {
    def apply(byte: Byte): Boolean = throw new Exception("boom!")
  }

  def newConsumeAllProcessor = new Buf.Processor {
    def apply(byte: Byte): Boolean = true
  }

  def newStopAtProcessor(stopAt: Int) = new Buf.Processor {
    var idx = 0

    def apply(byte: Byte): Boolean = {
      if (idx == stopAt) false
      else {
        idx += 1
        true
      }
    }
  }
}

abstract class ReadableBufProcessorTest(
  processable: String,
  newProcessable: (Array[Byte] => ReadableBufProcessorTest.CanProcess))
    extends AnyFunSuite
    with ScalaCheckDrivenPropertyChecks {
  import ReadableBufProcessorTest._

  test(s"$processable: process throws exception when `from` < 0") {
    val b = newProcessable(new Array[Byte](3))
    intercept[IllegalArgumentException] {
      b.process(-1, 2, newConsumeAllProcessor)
    }
  }

  test(s"$processable: process throws exception when `to` < 0") {
    val b = newProcessable(new Array[Byte](3))
    intercept[IllegalArgumentException] {
      b.process(1, -2, newConsumeAllProcessor)
    }
  }

  test(s"$processable: process returns -1 when empty underlying") {
    val b = newProcessable(new Array[Byte](0))
    assert(b.process(newExceptionalProcessor()) == -1)
  }

  test(s"$processable: process returns -1 when until < from") {
    val b = newProcessable(new Array[Byte](3))
    assert(b.process(2, 1, newExceptionalProcessor()) == -1)
  }

  test(s"$processable: process returns -1 when `from` > readable bytes") {
    forAll { bytes: Array[Byte] =>
      val b = newProcessable(bytes)
      val read = Math.floor(bytes.length / 2).toInt
      b.readBytes(read)
      val readable = bytes.length - read
      assert(b.process(readable + 1, readable + 2, newExceptionalProcessor()) == -1)
    }
  }

  test(s"$processable: process returns index where processing stopped") {
    forAll { bytes: Array[Byte] =>
      val b = newProcessable(bytes)
      assert(b.process(newStopAtProcessor(0)) == (if (bytes.isEmpty) -1 else 0))

      if (bytes.length <= 3) {
        assert(-1 == b.process(newStopAtProcessor(3)))
      } else {
        assert(3 == b.process(newStopAtProcessor(3)))
        if (bytes.length > 10) {
          assert(4 == b.process(1, 5, newStopAtProcessor(3)))
          assert(5 == b.process(2, 9, newStopAtProcessor(3)))
          assert(-1 == b.process(0, 3, newStopAtProcessor(3)))
        }
      }
    }
  }

  test("process returns -1 when fully processed") {
    forAll { bytes: Array[Byte] =>
      val b = newProcessable(bytes)
      var n = 0
      val processor = new Buf.Processor {
        def apply(byte: Byte): Boolean = {
          n += 1
          true
        }
      }
      assert(b.process(processor) == -1)
      assert(bytes.length == n)
    }
  }

  test(s"$processable: process will not go past the end of the buf") {
    forAll { bytes: Array[Byte] =>
      whenever(bytes.length > 0) {
        val b = newProcessable(bytes)
        assert(b.process(0, bytes.length, newConsumeAllProcessor) == -1)
        assert(b.process(0, bytes.length + 1, newConsumeAllProcessor) == -1)
        assert(b.process(1, bytes.length + 10, newConsumeAllProcessor) == -1)
      }
    }
  }

  test(s"$processable: process will not fail if offset > readable bytes") {
    val b = newProcessable(new Array[Byte](20))
    b.readBytes(10)
    assert(b.process(2, 3, newConsumeAllProcessor) == -1)
  }

  test(s"$processable: process handles readerIndex") {
    forAll { bytes: Array[Byte] =>
      whenever(bytes.length > 1) {
        val b = newProcessable(bytes)
        b.readBytes(0)
        assert(b.process(newStopAtProcessor((0))) == 0)
        if (bytes.length >= 2) {
          assert(b.process(1, 2, newStopAtProcessor(0)) == 1)
        }
      }
    }
  }

  test(s"$processable: process does not change readerIndex") {
    forAll { bytes: Array[Byte] =>
      whenever(bytes.length > 0) {
        val b = newProcessable(bytes)
        assert(b.readerIndex == 0)
        assert(b.process(newConsumeAllProcessor) == -1)
        assert(b.readerIndex == 0)
      }
    }
  }
}
