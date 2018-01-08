package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol.ServerError
import com.twitter.finagle.memcached.protocol.text.FrameDecoder
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf
import com.twitter.logging.Logger
import scala.collection.mutable

private object ClientDecoder {
  private val log = Logger.get()

  private val End: Buf = Buf.Utf8("END")
  private val Item: Buf = Buf.Utf8("ITEM")
  private val Stat: Buf = Buf.Utf8("STAT")
  private val Value: Buf = Buf.Utf8("VALUE")

  private def isEnd(tokens: Seq[Buf]): Boolean =
    tokens.length == 1 && tokens.head == End

  private def isStats(tokens: Seq[Buf]): Boolean = {
    if (tokens.isEmpty) false
    else
      tokens.head match {
        case Stat | Item => true
        case _ => false
      }
  }

  private def validateValueResponse(args: Seq[Buf]): Unit = {
    if (args.length < 4) throw new ServerError("Too few arguments")
    if (args.length > 5) throw new ServerError("Too many arguments")
    if (args.length == 5 && !ParserUtils.isDigits(args(4)))
      throw new ServerError("CAS must be a number")
    if (!ParserUtils.isDigits(args(3))) throw new ServerError("Bytes must be number")
  }
}

/**
 * Decodes Buf-encoded protocol messages into protocol specific Responses. Used by the client.
 *
 * @note Class contains mutable state. Not thread-safe.
 */
private[finagle] abstract class ClientDecoder[R] extends FrameDecoder[R] {
  import ClientDecoder._

  /** Type that represents a complete cache value */
  protected type Value

  /** Sequence of tokens that represents a text line */
  final protected type Tokens = Seq[Buf]

  private sealed trait State
  private case object AwaitingResponse extends State
  private case class AwaitingResponseOrEnd(valuesSoFar: Seq[Value]) extends State
  private case class AwaitingStatsOrEnd(valuesSoFar: Seq[Tokens]) extends State
  private case class AwaitingData(valuesSoFar: Seq[Value], tokens: Seq[Buf], bytesNeeded: Int)
      extends State
  private case class Failed(error: Throwable) extends State

  private[this] var state: State = AwaitingResponse

  /** Parse a sequence of tokens into a response */
  protected def parseResponse(tokens: Seq[Buf]): R

  /** Parse a text line, its associated data, and the casUnique into a Value */
  protected def parseValue(tokens: Seq[Buf], data: Buf): Value

  /** Parse a collection of values into a single response */
  protected def parseResponseValues(values: Seq[Value]): R

  /** Parse a collection of token sequences into a single response */
  protected def parseStatLines(lines: Seq[Tokens]): R

  final def nextFrameBytes(): Int = state match {
    case AwaitingData(_, _, bytesNeeded) => bytesNeeded
    case _ => -1
  }

  final def decodeData(buffer: Buf, results: mutable.Buffer[R]): Unit = state match {
    case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
      // The framer should have given us the right sized Buf
      if (buffer.length != bytesNeeded) {
        throw new IllegalArgumentException(
          s"Expected to receive a buffer of $bytesNeeded bytes but " +
            s"only received ${buffer.length} bytes"
        )
      }

      state = AwaitingResponseOrEnd(valuesSoFar :+ parseValue(tokens, buffer))

    case AwaitingResponse =>
      val tokens = ParserUtils.splitOnWhitespace(buffer)
      val dataBytes = needsData(tokens)
      if (dataBytes == -1) {
        if (isEnd(tokens)) {
          results += parseResponseValues(Nil)
        } else if (isStats(tokens)) {
          state = AwaitingStatsOrEnd(Vector(tokens))
        } else {
          results += parseResponse(tokens)
        }
      } else {
        // We are waiting for data next
        state = AwaitingData(Nil, tokens, dataBytes)
      }

    case AwaitingStatsOrEnd(linesSoFar) =>
      val tokens = ParserUtils.splitOnWhitespace(buffer)
      if (isEnd(tokens)) {
        state = AwaitingResponse
        results += parseStatLines(linesSoFar)
      } else if (isStats(tokens)) {
        state = AwaitingStatsOrEnd(linesSoFar :+ tokens)
      } else {
        val ex = new ServerError("Invalid reply from STATS command")
        state = Failed(ex)
        throw ex
      }

    case AwaitingResponseOrEnd(valuesSoFar) =>
      val tokens = ParserUtils.splitOnWhitespace(buffer)
      val bytesNeeded = needsData(tokens)
      if (bytesNeeded == -1) {
        if (isEnd(tokens)) {
          state = AwaitingResponse
          results += parseResponseValues(valuesSoFar)
        } else {
          // This is a problem: if it wasn't a value line, it should have been an END.
          val bufString =
            tokens.foldLeft("") { (acc, buffer) =>
              acc + Buf.Utf8.unapply(buffer).getOrElse("<non-string token>") + " "
            }

          val ex = new ServerError(
            s"Server returned invalid response when values or END was expected: $bufString"
          )
          state = Failed(ex)
          throw ex
        }
      } else {
        state = AwaitingData(valuesSoFar, tokens, bytesNeeded)
      }

    case Failed(cause) =>
      val msg = "Failed Memcached decoder called after previous decoding failure."
      val ex = new IllegalStateException(msg, cause)
      log.error(msg, ex)
      throw ex
  }

  private[this] def needsData(tokens: Seq[Buf]): Int = {
    if (tokens.isEmpty) -1
    else {
      val responseName = tokens.head
      if (responseName == Value) {
        validateValueResponse(tokens)
        val dataLengthAsBuf = tokens(3)
        ParserUtils.bufToInt(dataLengthAsBuf)
      } else -1
    }
  }
}
