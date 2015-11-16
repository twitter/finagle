package com.twitter.finagle.redis.naggati

import com.twitter.conversions.time._
import com.twitter.finagle.redis.{ClientError, ServerError}
import com.twitter.finagle.redis.naggati.test._
import com.twitter.finagle.redis.util._
import com.twitter.util.Time
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
  test("Correctly decode OK status reply") {
    assert(codec(wrap("+OK\r\n")) == List(StatusReply("OK")))
  }

  test("Correctly decode OK status reply with message") {
    assert(codec(wrap("+OK\r\n+Hello World\r\n")) ==
      List(StatusReply("OK"), StatusReply("Hello World")))
  }

  test("Throw ServerError when decoding BLANK OK reply") {
    intercept[ServerError] {
      codec(wrap("+\r\n"))
    }
  }

  test("Correctly decode BAD error reply") {
    assert(codec(wrap("-BAD\r\n")) == List(ErrorReply("BAD")))
  }

  test("Correctly decode BAD error reply with message") {
    assert(codec(wrap("-BAD\r\n-Bad Thing\r\n")) ==
      List(ErrorReply("BAD"), ErrorReply("Bad Thing")))
  }

  test("Throw ServerError when decoding BLANK error reply") {
    intercept[ServerError] {
      codec(wrap("-\r\n"))
    }
  }

  test("Correctly decode negative integer reply") {
    assert(codec(wrap(":-2147483648\r\n")) == List(IntegerReply(-2147483648)))
  }

  test("Correctly decode zero integer reply") {
    assert(codec(wrap(":0\r\n")) == List(IntegerReply(0)))
  }

  test("Correctly decode positive integer reply") {
    assert(codec(wrap(":2147483647\r\n")) == List(IntegerReply(2147483647)))
  }

  test("Correctly decode positive integer to long reply") {
    assert(codec(wrap(":2147483648\r\n")) == List(IntegerReply(2147483648L)))
  }

  test("Correctly decode negative integer to long reply") {
    assert(codec(wrap(":-2147483649\r\n")) == List(IntegerReply(-2147483649L)))
  }

  test("Correctly decode Long.MaxValue reply") {
    assert(codec(wrap(":9223372036854775807\r\n")) == List(IntegerReply(9223372036854775807L)))
  }

  test("Throw ServerError when decoding Long.MaxValue + 1") {
    intercept[ServerError] {
      codec(wrap(":9223372036854775808\r\n"))
    }
  }

  test("Correctly decode Long.MinValue") {
    assert(codec(wrap(":-9223372036854775807\r\n")) == List(IntegerReply(-9223372036854775807L)))
  }

  test("Throw ServerError when decoding Long.MinValue -1") {
    intercept[ServerError] {
      codec(wrap(":-9223372036854775809\r\n"))
    }
  }

  test("Correctly decode multiple integers in one reply") {
    assert(codec(wrap(":1\r\n:2\r\n")) == List(IntegerReply(1), IntegerReply(2)))
  }

  test("Throw ServerError when decoding BLANK integer reply") {
    intercept[ServerError] {
      codec(wrap(":\r\n"))
    }
  }

  test("Correctly decode single word bulk reply") {
    val decoding = decomposeSingleElemDecoding(codec(wrap("$3\r\nfoo\r\n")))
    assert(decoding._1 == "foo", decoding._2)
  }

  test("Correctly decode multi word bulk reply") {
    val decoding = decomposeSingleElemDecoding(codec(wrap("$8\r\nfoo\r\nbar\r\n")))
    assert(decoding._1 == "foo\r\nbar", decoding._2)
  }

  test("Correctly decodec multi line reply") {
    assert(decomposeMultiElemDecoding(codec(wrap("$3\r\nfoo\r\n$3\r\nbar\r\n"))) == ("foo", "bar"))
  }

  test("Correctly decode EMPTY bulk reply") {
    assert(codec(wrap("$-1\r\n")).head.getClass == classOf[EmptyBulkReply])
  }

  test("Correctly decode empty multi-bulk replies") {
    assert(codec(wrap("*0\r\n")).head.getClass == classOf[EmptyMBulkReply])
  }

  // First multi-bulk array
  test("Correctly decode first multi-bulk array size replies") {
    assert(codec(wrap("*4\r\n")) == Nil)
  }

  test("Correctly decode first multi-bulk string size replies 1") {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly decode first multi-bulk string replies 1") {
    assert(codec(wrap("foo\r\n")) == Nil)
  }

  test("Correctly decode first multi-bulk string size replies 2") {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly decode first multi-bulk string replies 2") {
    assert(codec(wrap("bar\r\n")) == Nil)
  }

  test("Correctly decode first multi-bulk string size replies 3") {
    assert(codec(wrap("$5\r\n")) == Nil)
  }

  test("Correctly decode first multi-bulk string replies 3") {
    assert(codec(wrap("Hello\r\n")) == Nil)
  }

  test("Correctly decode first multi-bulk string size replies 4") {
    assert(codec(wrap("$5\r\n")) == Nil)
  }

  test("Correctly decode first multi-bulk messages") {
    codec(wrap("World\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(msgs) =>
          assert(ReplyFormat.toString(msgs) == List("foo", "bar", "Hello", "World"))
        case _ => fail("Expected MBulkReply")
      }
      case _ => fail("Expected one element in list")
    }
  }

  // Second multi-bulk array (with null)
  test("Correctly decode second multi-bulk array size replies") {
    assert(codec(wrap("*3\r\n")) == Nil)
  }

  test("Correctly decode second multi-bulk string size replies 1") {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly decode second multi-bulk string replies 1") {
    assert(codec(wrap("foo\r\n")) == Nil)
  }

  test("Correctly decode second multi-bulk null string replies") {
    assert(codec(wrap("$-1\r\n")) == Nil)
  }

  test("Correctly decode second multi-bulk string size replies 2") {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly decode second multi-bulk messages") {
    codec(wrap("bar\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(msgs) =>
          assert(ReplyFormat.toString(msgs) == List("foo", "nil", "bar"))
        case _ => fail("Expected MBulkReply")
      }
      case _ => fail("Expected one element in list")
    }
  }

  // Third multi-bulk array (with empty string)
  test("Correctly decode third multi-bulk array size replies") {
    assert(codec(wrap("*4\r\n")) == Nil)
  }

  test("Correctly decode third multi-bulk string size replies 1") {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly decode third multi-bulk string replies 1") {
    assert(codec(wrap("foo\r\n")) == Nil)
  }

  test("Correctly decode third multi-bulk string size replies 2") {
    assert(codec(wrap("$0\r\n")) == Nil)
  }

  test("Correctly decode third multi-bulk string replies 2") {
    assert(codec(wrap("\r\n")) == Nil)
  }

  test("Correctly decode third multi-bulk string size replies 3") {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly decode third multi-bulk string replies 3") {
    assert(codec(wrap("moo\r\n")) == Nil)
  }

  test("Correctly decode third multi-bulk string size replies 4") {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly decode third multi-bulk messagess") {
    codec(wrap("bar\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(msgs) =>
          assert(ReplyFormat.toString(msgs) == List("foo", "", "moo", "bar"))
        case _ => fail("Expected MBulkReply")
      }
      case _ => fail("Expected one element in list")
    }
  }

  // First nested multi-bulk array
  test("Correctly decode first nested multi-bulk array size replies") {
    assert(codec(wrap("*3\r\n")) == Nil)
  }

  test("Correctly decode first nested multi-bulk integer replies 1") {
    assert(codec(wrap(":1\r\n")) == Nil)
  }

  test("Correctly decode first nested multi-bulk inner array size replies") {
    assert(codec(wrap("*2\r\n")) == Nil)
  }

  test("Correctly decode first nested multi-bulk string size replies 1") {
    assert(codec(wrap("$3\r\n")) == Nil)
  }

  test("Correctly decode first nested multi-bulk string replies 1") {
    assert(codec(wrap("one\r\n")) == Nil)
  }

  test("Correctly decode first nested multi-bulk string size replies 2") {
    assert(codec(wrap("$5\r\n")) == Nil)
  }

  test("Correctly decode first nested multi-bulk string replies 2") {
    assert(codec(wrap("three\r\n")) == Nil)
  }

  test("Correctly decode first nested multi-bulk messages") {
    codec(wrap(":3\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(List(a, b, c)) =>
          assert(a == IntegerReply(1))
          b match {
            case MBulkReply(xs) =>
              assert(ReplyFormat.toString(xs) == List("one", "three"))
            case xs => fail("Expected MBulkReply, got: %s" format xs)
          }
          assert(c == IntegerReply(3))
        case xs => fail("Expected 3-element MBulkReply, got: %s" format xs)
      }
      case xs => fail("Expected one reply, got: %s" format xs)
    }
  }

  // Second nested multi-bulk array
  test("Correctly decode second nested multi-bulk array size replies") {
    assert(codec(wrap("*4\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk integer replies 1") {
    assert(codec(wrap(":0\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk integer replies 2") {
    assert(codec(wrap(":1\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk inner array size replies 1") {
    assert(codec(wrap("*3\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk integer replies 3") {
    assert(codec(wrap(":10\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk inner array size replies 2") {
    assert(codec(wrap("*0\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk inner array size replies 3") {
    assert(codec(wrap("*2\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk inner array size replies 4") {
    assert(codec(wrap("*0\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk integer replies 4") {
    assert(codec(wrap(":100\r\n")) == Nil)
  }

  test("Correctly decode second nested multi-bulk messages") {
    codec(wrap(":2\r\n")) match {
      case reply :: Nil => reply match {
        case MBulkReply(List(a, b, c, d)) =>
          assert(a == IntegerReply(0))
          assert(b == IntegerReply(1))
          c match {
            case MBulkReply(List(aa, ab, ac)) =>
              assert(aa == IntegerReply(10))
              assert(ab == EmptyMBulkReply())
              ac match {
                case MBulkReply(List(aaa, aab)) =>
                  assert(aaa == EmptyMBulkReply())
                  assert(aab == IntegerReply(100))
                case xs => fail("Expected 2-element MBulkReply, got: %s" format xs)
              }
            case xs => fail("Expected 3-element, got: %s" format xs)
          }
          assert(d == IntegerReply(2))
        case xs => fail("Expected 4-element MBulkReply, got: %s" format xs)
      }
      case xs => fail("Expected one reply, got: %s" format xs)
    }
  }

  private[this] def decomposeSingleElemDecoding(reply: List[AnyRef]): (String, String) =
    reply match {
      case reply :: Nil => reply match {
        case BulkReply(msg) => (CBToString(msg), "")
        case _ => ("USE FAILURE MSG, NOT THIS VALUE", "Expected BulkReply, got something else")
      }
      case _ => ("USE FAILURE MSG, NOT THIS VALUE", "Found no or multiple reply lines")
    }

  private[this] def decomposeMultiElemDecoding(reply: List[AnyRef]): (String, String) =
    reply match {
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
