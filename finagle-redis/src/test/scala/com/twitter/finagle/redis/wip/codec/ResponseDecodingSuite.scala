package com.twitter.finagle.redis.naggati

import com.twitter.conversions.time._
import com.twitter.finagle.redis.{ClientError, ServerError}
import com.twitter.finagle.redis.naggati.test._
import com.twitter.finagle.redis.util._
import com.twitter.util.Time
import org.specs.SpecificationWithJUnit
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.redis.protocol.{BulkReply, ErrorReply, IntegerReply, MBulkReply,
                                          StatusReply, ReplyCodec}
import com.twitter.finagle.redis.util.StringToChannelBuffer
import org.jboss.netty.buffer.ChannelBuffer
import org.scalatest.FunSuite
import com.twitter.finagle.redis.protocol.{EmptyBulkReply, EmptyMBulkReply}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
final class ResponseDecodingSuite extends RedisResponseTest {
  ignore("Multi-bulk replies") {
    val actualEncoding = codec(wrap("*4\r\n"))
    val expectedEncoding = Nil
    assert(actualEncoding === expectedEncoding)
  }

  ignore("Correctly decode faulty") {
    val actualEncoding = codec(wrap("$3\r\n"))
    val expectedEncoding = Nil
    assert(actualEncoding === expectedEncoding)
  }

  ignore("Correctly decode ") {
    val actualEncoding = Nil
    val expectedEncoding = Nil
    assert(actualEncoding === expectedEncoding)
  }


  /*"multi-bulk replies" >> {
    codec(wrap("*4\r\n")) mustEqual Nil
    codec(wrap("$3\r\n")) mustEqual Nil
    codec(wrap("foo\r\n")) mustEqual Nil
    codec(wrap("$3\r\n")) mustEqual Nil
    codec(wrap("bar\r\n")) mustEqual Nil
    codec(wrap("$5\r\n")) mustEqual Nil
    codec(wrap("Hello\r\n")) mustEqual Nil
    codec(wrap("$5\r\n")) mustEqual Nil
    codec(wrap("World\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(msgs) =>
          ReplyFormat.toString(msgs) mustEqual List("foo","bar","Hello","World")
        case _ => fail("Expected MBulkReply")
      }
      case _ => fail("Expected one element in list")
    }

    codec(wrap("*3\r\n")) mustEqual Nil
    codec(wrap("$3\r\n")) mustEqual Nil
    codec(wrap("foo\r\n")) mustEqual Nil
    codec(wrap("$-1\r\n")) mustEqual Nil
    codec(wrap("$3\r\n")) mustEqual Nil
    codec(wrap("bar\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(msgs) =>
          ReplyFormat.toString(msgs) mustEqual List(
            "foo",
            "nil",
            "bar")
        case _ => fail("Expected MBulkReply")
      }
      case _ => fail("Expected one element in list")
    }

    codec(wrap("*4\r\n")) mustEqual Nil
    codec(wrap("$3\r\n")) mustEqual Nil
    codec(wrap("foo\r\n")) mustEqual Nil
    codec(wrap("$0\r\n")) mustEqual Nil
    codec(wrap("\r\n")) mustEqual Nil
    codec(wrap("$3\r\n")) mustEqual Nil
    codec(wrap("moo\r\n")) mustEqual Nil
    codec(wrap("$3\r\n")) mustEqual Nil
    codec(wrap("bar\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(msgs) =>
          ReplyFormat.toString(msgs) mustEqual List(
            "foo",
            "",
            "moo",
            "bar")
        case _ => fail("Expected MBulkReply")
      }
      case _ => fail("Expected one element in list")
    }

  }
  "nested multi-bulk replies" >> {
    codec(wrap("*3\r\n")) mustEqual Nil
    codec(wrap(":1\r\n")) mustEqual Nil
    codec(wrap("*2\r\n")) mustEqual Nil
    codec(wrap("$3\r\n")) mustEqual Nil
    codec(wrap("one\r\n")) mustEqual Nil
    codec(wrap("$5\r\n")) mustEqual Nil
    codec(wrap("three\r\n")) mustEqual Nil
    codec(wrap(":3\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(List(a, b, c)) =>
          a mustEqual IntegerReply(1)
          b match {
            case MBulkReply(xs) =>
              ReplyFormat.toString(xs) mustEqual List("one", "three")
            case xs => fail("Expected MBulkReply, got: %s" format xs)
          }
          c mustEqual IntegerReply(3)
        case xs => fail("Expected 3-element MBulkReply, got: %s" format xs)
      }
      case xs => fail("Expected one reply, got: %s" format xs)
    }

    codec(wrap("*4\r\n")) mustEqual Nil
    codec(wrap(":0\r\n")) mustEqual Nil
    codec(wrap(":1\r\n")) mustEqual Nil
    codec(wrap("*3\r\n")) mustEqual Nil
    codec(wrap(":10\r\n")) mustEqual Nil
    codec(wrap("*0\r\n")) mustEqual Nil
    codec(wrap("*2\r\n")) mustEqual Nil
    codec(wrap("*0\r\n")) mustEqual Nil
    codec(wrap(":100\r\n")) mustEqual Nil
    codec(wrap(":2\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(List(a, b, c, d)) =>
          a mustEqual IntegerReply(0)
          b mustEqual IntegerReply(1)
          c match {
            case MBulkReply(List(aa, ab, ac)) =>
              aa mustEqual IntegerReply(10)
              ab mustEqual EmptyMBulkReply()
              ac match {
                case MBulkReply(List(aaa, aab)) =>
                  aaa mustEqual EmptyMBulkReply()
                  aab mustEqual IntegerReply(100)
                case xs => fail("Expected 2-element MBulkReply, got: %s" format xs)
              }
            case xs => fail("Expected 3-element, got: %s" format xs)
          }
          d mustEqual IntegerReply(2)
        case xs => fail("Expected 4-element MBulkReply, got: %s" format xs)
      }
      case xs => fail("Expected one reply, got: %s" format xs)
    }
  }
}*/
  ignore("Correctly decode OK status reply") {
    val actualDecoding = codec(wrap("+OK\r\n"))
    val expectedDecoding = List(StatusReply("OK"))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Correctly decode OK status reply with message") {
    val actualDecoding = codec(wrap("+OK\r\n+Hello World\r\n"))
    val expectedDecoding = List(StatusReply("OK"), StatusReply("Hello World"))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Throw ServerError when decoding BLANK OK reply") {
    intercept[ServerError] {
      codec(wrap("+\r\n"))
    }
  }

  ignore("Correctly decode BAD error reply") {
    val actualDecoding = codec(wrap("-BAD\r\n"))
    val expectedDecoding = List(ErrorReply("BAD"))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Correctly decode BAD error reply with message") {
    val actualDecoding = codec(wrap("-BAD\r\n-Bad Thing\r\n"))
    val expectedDecoding = List(ErrorReply("BAD"), ErrorReply("Bad Thing"))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Throw ServerError when decoding BLANK error reply") {
    intercept[ServerError] {
      codec(wrap("-\r\n"))
    }
  }

  ignore("Correctly decode negative integer reply") {
    val actualDecoding = codec(wrap(":-2147483648\r\n"))
    val expectedDecoding = List(IntegerReply(-2147483648))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Correctly decode zero integer reply") {
    val actualDecoding = codec(wrap(":0\r\n"))
    val expectedDecoding = List(IntegerReply(0))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Correctly decode positive integer reply") {
    val actualDecoding = codec(wrap(":2147483647\r\n"))
    val expectedDecoding = List(IntegerReply(2147483647))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Correctly decode positive integer to long reply") {
    val actualDecoding = codec(wrap(":2147483648\r\n"))
    val expectedDecoding = List(IntegerReply(2147483648L))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Correctly decode negative integer to long reply") {
    val actualDecoding = codec(wrap(":-2147483649\r\n"))
    val expectedDecoding = List(IntegerReply(-2147483649L))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Correctly decode Long.MaxValue reply") {
    val actualDecoding = codec(wrap(":9223372036854775807\r\n"))
    val expectedDecoding = List(IntegerReply(9223372036854775807L))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Throw ServerError when decoding Long.MaxValue + 1") {
    intercept[ServerError] {
      codec(wrap(":9223372036854775808\r\n"))
    }
  }

  ignore("Correctly decode Long.MinValue") {
    val actualDecoding = codec(wrap(":-9223372036854775807\r\n"))
    val expectedDecoding = List(IntegerReply(-9223372036854775807L))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Throw ServerError when decoding Long.MinValue -1") {
    intercept[ServerError] {
      codec(wrap(":-9223372036854775809\r\n"))
    }
  }

  ignore("Correctly decode multiple integers in one reply") {
    val actualDecoding = codec(wrap(":1\r\n:2\r\n"))
    val expectedDecoding = List(IntegerReply(1), IntegerReply(2))
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Throw ServerError when decoding BLANK integer reply") {
    intercept[ServerError] {
      codec(wrap(":\r\n"))
    }
  }

  ignore("Correctly decode single word bulk reply") {
    val actualDecoding = decomposeSingleElemDecoding(codec(wrap("$3\r\nfoo\r\n")))
    val expectedDecoding = "foo"
    assert(actualDecoding._1 === expectedDecoding, actualDecoding._2)
  }

  ignore("Correctly decode multi word bulk reply") {
    val actualDecoding = decomposeSingleElemDecoding(codec(wrap("$8\r\nfoo\r\nbar\r\n")))
    val expectedDecoding = "foo\r\nbar"
    assert(actualDecoding._1 === expectedDecoding, actualDecoding._2)
  }

  ignore("Correctly decodec multi line reply") {
    val actualDecoding = decomposeMultiElemDecoding(codec(wrap("$3\r\nfoo\r\n$3\r\nbar\r\n")))
    val expectedDecoding = ("foo", "bar")
    assert(actualDecoding === expectedDecoding)
  }

  ignore("Correctly decode EMPTY bulk reply") {
    val actualDecodingClass = codec(wrap("$-1\r\n")).head.getClass
    val expectedDecodingClass = classOf[EmptyBulkReply]
    assert(actualDecodingClass === expectedDecodingClass)
  }

  ignore("Correctly decode empty multi-bulk replies") {
    val actualEmptyDecoding = codec(wrap("*0\r\n")).head.getClass
    val expectedEmptyDecoding = classOf[EmptyMBulkReply]
    assert(actualEmptyDecoding === expectedEmptyDecoding)
  }

  private[this] def decomposeSingleElemDecoding(reply: List[AnyRef]): (String, String) = reply match {
    case reply :: Nil => reply match {
      case BulkReply(msg) => (CBToString(msg), "")
      case _ => ("USE FAILURE MSG, NOT THIS VALUE", "Expected BulkReply, got something else")
      }
      case _ => ("USE FAILURE MSG, NOT THIS VALUE", "Found no or multiple reply lines")
    }

  private[this] def decomposeMultiElemDecoding(reply: List[AnyRef]): (String, String) = reply match {
    case fooR :: booR :: Nil => {
      val fooMsg = fooR match {
        case BulkReply(msg) => CBToString(msg)
        case _              => "Expected Bulk Reply"
      }
      val barMsg = booR match {
        case BulkReply(msg) => CBToString(msg)
        case _              => "Expected Bulk Reply"
      }
      (fooMsg, barMsg)
    }
    case _ => fail("Expected two element in list")
  }
}
