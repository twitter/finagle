package com.twitter.finagle

import java.net.SocketAddress

import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.StdStackClient
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.decoder.Framer
import com.twitter.finagle.decoder.LengthFieldFramer
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.postgresql.Params
import com.twitter.finagle.postgresql.Request
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.io.Buf

object PostgreSql {

  val defaultStack: Stack[ServiceFactory[Request, Response]] = StackClient.newStack[Request, Response]
  val defaultParams: Stack.Params = StackClient.defaultParams

  case class Client(
    stack:  Stack[ServiceFactory[Request, Response]] = defaultStack,
    params: Stack.Params = defaultParams
  ) extends StdStackClient[Request, Response, Client] {

    override type In = Buf
    override type Out = Buf
    override type Context = TransportContext

    type ClientTransport = Transport[In, Out] { type Context <: TransportContext }

    def withCredentials(u: String, p: Option[String]): Client =
      configured(Params.Credentials(u, p))

    def withDatabase(db: String): Client =
      configured(Params.Database(Some(db)))

    override protected def newTransporter(addr: SocketAddress): Transporter[Buf, Buf, TransportContext] = {
      // TODO: this doesn't work during ssl handshaking
      def factory: Framer =
        new LengthFieldFramer(
          lengthFieldBegin = 1,
          lengthFieldLength = 4,
          lengthAdjust = 1,
          maxFrameLength = Int.MaxValue,
          bigEndian = true)

      Netty4Transporter.framedBuf(Some(factory _), addr, params)
    }

    override protected def newDispatcher(transport: ClientTransport): Service[Request, Response] =
      new postgresql.ClientDispatcher(transport.map[Packet, Packet](_.toBuf, Packet.parse), params)

    override protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)
  }

}
