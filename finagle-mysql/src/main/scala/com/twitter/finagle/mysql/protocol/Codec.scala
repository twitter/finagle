package com.twitter.finagle.mysql.protocol

import com.twitter.finagle.mysql._
import collection.immutable.Queue
import com.twitter.finagle.mysql.util._
import com.twitter.util.StateMachine
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.frame.FrameDecoder
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

trait Packet {
  def size: Int
  def number: Byte
  def data: Array[Byte]
}

object Decoder {
  val EOF_Byte = 0xFE.toByte
  val OK_Byte = 0x00.toByte
  val ERROR_Byte = 0xFF.toByte
}

class Decoder extends FrameDecoder with StateMachine {
  import Decoder._

  case object WaitingForGreetings extends State
  case object WaitingForLoginResponse extends State
  case object Idle extends State
  case class QueryInProgress(
    header: Option[Packet] = None,
    fields: Option[List[Packet]] = None,
    data: Option[List[Packet]] = None
  ) extends State

  val needMoreData: Packet = null
  state = WaitingForGreetings

  def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Result = {
    state match {
      case WaitingForGreetings =>
        state = WaitingForLoginResponse
        ServerGreetings.decode(decodePacket(buffer))

      case WaitingForLoginResponse =>
        val res = decodeResult(buffer)
        if (res == Error)
          state = WaitingForGreetings
        res

      case Idle =>
        decodeResult(buffer)

      case QueryInProgress(_,_,_) =>
        val res = decodeResultSet(decodePacket(buffer), buffer)
        if (res != null)
          state = Idle
        res
    }
  }

  def decodeResult(buffer: ChannelBuffer) = {
    val packet = decodePacket(buffer)
    packet.data(0) match {
      case `OK_Byte` =>
        state = Idle
        OK
      case `ERROR_Byte`=>
        state = Idle
        Error
      case b =>
        state = QueryInProgress()
        decodeResultSet(packet, buffer)
    }
  }

  def decodeResultSet(packet: Packet, buffer: ChannelBuffer): Result = {
    if (packet == null)
      null // Need more data
    else
      (state, packet.data(0)) match {
        case (QueryInProgress(None, _, _), _) =>
          state = QueryInProgress(Some(packet), Some(Nil), None)
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(Some(h), Some(xs), None), `EOF_Byte`) =>
          state = QueryInProgress(Some(h), Some(xs), Some(Nil))
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(Some(h), Some(xs), None), _) =>
          state = QueryInProgress(Some(h), Some(packet :: xs), None)
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(Some(h), Some(xs), Some(ys)), `EOF_Byte`) =>
          state = QueryInProgress()
          ResultSet.decode(h, xs.reverse, ys.reverse)

        case (QueryInProgress(Some(h), Some(xs), Some(ys)), _) =>
          state = QueryInProgress(Some(h), Some(xs), Some(packet :: ys))
          decodeResultSet(decodePacket(buffer), buffer)

        case (QueryInProgress(_, _, _), _) =>
          throw new Exception("State inconcistency !!!")
    }
  }

  def decodePacket(buffer: ChannelBuffer): Packet = {
    if (buffer.readableBytes < 4)
      needMoreData
    else {
      var s: Int = buffer.readByte()
      s += buffer.readByte() << 8
      s += buffer.readByte() << 16
      val n = buffer.readByte()
      if (buffer.readableBytes() < s)
        needMoreData
      else {
        println("<- Decoding MySQL packet (n=%d, size=%d)".format(n,s))
        val _data = new Array[Byte](s)
        buffer.readBytes(_data)
        Util.hex(_data)
        new Packet {
          val size = s
          val number = n
          val data = _data
        }
      }
    }
  }
}

object Encoder extends OneToOneEncoder {
  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case req: Request =>
        val encodedMsg = req.encode()
        println("-> Encoding " + req)
        Util.hex(encodedMsg)
        ChannelBuffers.wrappedBuffer(encodedMsg)
      case _ =>
        message
    }
  }
}

class AuthenticationHandler(login: String, password: String) extends SimpleChannelHandler with
StateMachine {
  case class WaitingForGreetings(requestQueue: Queue[Request] = Queue.empty[Request]) extends State
  case class WaitingForLoginResponse(requestQueue: Queue[Request]) extends State
  case class Authenticated() extends State

  override def channelBound(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = WaitingForGreetings()
    super.channelBound(ctx, e)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    state = WaitingForGreetings()
    super.channelClosed(ctx, e)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    (e.getMessage, state) match {
      case (greetings: ServerGreetings, WaitingForGreetings(queue)) =>
        val loginRequest = LoginRequest(
          username = login,
          password = password,
          salt = greetings.salt
        )
        val encLog = loginRequest.encode()
        println("-> Encoding LoginRequest")
        Util.hex(encLog)
        Channels.write(ctx, Channels.future(ctx.getChannel), ChannelBuffers.wrappedBuffer(encLog))
        state = WaitingForLoginResponse(queue)

      case (result: Result, WaitingForLoginResponse(queue)) =>
        result match {
          case OK =>
            state = Authenticated()
            queue foreach { req =>
              val data = req.encode()
              println("-> Encoding " + req)
              Util.hex(data)
              Channels.write(ctx, Channels.future(ctx.getChannel), ChannelBuffers.wrappedBuffer(data))
            }
          case _ =>
            println("Login failed")
        }
      case (msg, Authenticated()) =>
        super.messageReceived(ctx, e)

      case _ =>
        println("Received something which is not a MySQL packet...")
    }
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    (e.getMessage, state) match {
      case (m: Request, WaitingForGreetings(queue)) =>
        println("Enqueuing request " + m)
        state = WaitingForGreetings(queue.enqueue(m))
      case (m: Request, WaitingForLoginResponse(queue)) =>
        println("Enqueuing request " + m)
        state = WaitingForLoginResponse(queue.enqueue(m))
      case (m, _) =>
        super.writeRequested(ctx, e)
    }
  }
}
