package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol.ServerError
import com.twitter.finagle.memcached.protocol.text._
import com.twitter.finagle.memcached.util.ParserUtils
import com.twitter.io.Buf
import com.twitter.logging.Logger

/**
 * Decodes Buf-encoded protocol messages into protocol specific Responses. Used by the client.
 *
 * @note Class contains mutable state. Not thread-safe.
 */
private[memcached] object ClientDecoder {
  private val END: Buf = Buf.Utf8("END")
  private val ITEM: Buf = Buf.Utf8("ITEM")
  private val STAT: Buf = Buf.Utf8("STAT")
  private val VALUE: Buf = Buf.Utf8("VALUE")

  private val NeedMoreData: Null = null

  private def isEnd(tokens: Seq[Buf]) =
    tokens.length == 1 && tokens.head == END

  private def isStats(tokens: Seq[Buf]) =
    tokens.nonEmpty && (tokens.head == STAT || tokens.head == ITEM)

  private def validateValueResponse(args: Seq[Buf]): Unit = {
    if (args.length < 4) throw new ServerError("Too few arguments")
    if (args.length > 5) throw new ServerError("Too many arguments")
    if (args.length == 5 && !ParserUtils.isDigits(args(4))) throw new ServerError("CAS must be a number")
    if (!ParserUtils.isDigits(args(3))) throw new ServerError("Bytes must be number")
  }
}

private[finagle] abstract class ClientDecoder[R >: Null] extends Decoder[R] {
  import ClientDecoder._

  /** Type that represents a complete cache value */
  protected type Value

  /** Sequence of tokens that represents a text line */
  final protected type Tokens = Seq[Buf]

  private sealed trait State
  private case object AwaitingResponse extends State
  private case class AwaitingResponseOrEnd(valuesSoFar: Seq[Value]) extends State
  private case class AwaitingStatsOrEnd(valuesSoFar: Seq[Tokens]) extends State
  private case class AwaitingData(valuesSoFar: Seq[Value], tokens: Seq[Buf], bytesNeeded: Int) extends State
  private case class Failed(error: Throwable) extends State

  private[this] val log = Logger.get
  private[this] val byteArrayForBuf2Int = ParserUtils.newByteArrayForBuf2Int()
  private[this] var state: State = AwaitingResponse

  /** Parse a sequence of tokens into a response */
  protected def parseResponse(tokens: Seq[Buf]): R

  /** Parse a text line, its associated data, and the casUnique into a Value */
  protected def parseValue(tokens: Seq[Buf], data: Buf): Value

  /** Parse a collection of values into a single response */
  protected def parseResponseValues(values: Seq[Value]): R

  /** Parse a collection of token sequences into a single response */
  protected def parseStatLines(lines: Seq[Tokens]): R

  def decode(buffer: Buf): R = state match {
    case AwaitingResponse =>
      val tokens = ParserUtils.splitOnWhitespace(buffer)
      val dataBytes = needsData(tokens)
      if (dataBytes == -1) {
        if (isEnd(tokens)) {
          parseResponseValues(Nil)
        } else if (isStats(tokens)) {
          state = AwaitingStatsOrEnd(Vector(tokens))
          NeedMoreData
        } else {
          parseResponse(tokens)
        }
      } else {
        // We are waiting for data next
        state = AwaitingData(Nil, tokens, dataBytes)
        NeedMoreData
      }

    case AwaitingStatsOrEnd(linesSoFar) =>
      val tokens = ParserUtils.splitOnWhitespace(buffer)
      if (isEnd(tokens)) {
        state = AwaitingResponse
        parseStatLines(linesSoFar)
      } else if (isStats(tokens)) {
        state = AwaitingStatsOrEnd(linesSoFar :+ tokens)
        NeedMoreData
      } else {
        val ex = new ServerError("Invalid reply from STATS command")
        state = Failed(ex)
        throw ex
      }

    case AwaitingData(valuesSoFar, tokens, bytesNeeded) =>
      // The framer should have given us the right sized Buf
      assert(buffer.length == bytesNeeded)
      state = AwaitingResponseOrEnd(valuesSoFar :+ parseValue(tokens, buffer))
      NeedMoreData

    case AwaitingResponseOrEnd(valuesSoFar) =>
      val tokens = ParserUtils.splitOnWhitespace(buffer)
      val bytesNeeded = needsData(tokens)
      if (bytesNeeded == -1) {
        if (isEnd(tokens)) {
          state = AwaitingResponse
          parseResponseValues(valuesSoFar)
        } else {
          // This is a problem: if it wasn't a value line, it should have been an END.
          val bufStr = Buf.Utf8.unapply(buffer).getOrElse("<non-string message>")
          val ex = new ServerError(s"Server returned invalid response when values or END was expected: ${bufStr}")
          state = Failed(ex)
          throw ex
        }
      } else {
        state = AwaitingData(valuesSoFar, tokens, bytesNeeded)
        NeedMoreData
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
      if (responseName == VALUE) {
        validateValueResponse(tokens)
        val dataLengthAsBuf = tokens(3)
        dataLengthAsBuf.write(byteArrayForBuf2Int, 0)
        ParserUtils.byteArrayStringToInt(byteArrayForBuf2Int, dataLengthAsBuf.length)
      } else -1
    }
  }
}
