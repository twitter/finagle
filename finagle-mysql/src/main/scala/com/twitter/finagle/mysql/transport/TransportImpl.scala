package com.twitter.finagle.mysql.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.framer.LengthFieldFramer
import com.twitter.finagle.mysql.Toggles
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.netty4.Netty4Transporter
import com.twitter.finagle.server.ServerInfo
import com.twitter.finagle.Stack
import com.twitter.finagle.toggle.Toggle
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * Responsible for the transport layer plumbing required to produce
 * a Transporter[Packet, Packet]. The current default is toggled from netty3
 * to netty4 by the "com.twitter.finagle.mysql.UseNetty4" toggle.
 *
 * TODO(jparker): Convert this to Transporter[Buf, Buf] and adjust accordingly.
 */
object TransportImpl {
  private val UseNetty4ToggleId: String = "com.twitter.finagle.mysql.UseNetty4"
  private val netty4Toggle: Toggle[Int] = Toggles(UseNetty4ToggleId)
  private def useNetty4: Boolean = netty4Toggle(ServerInfo().id.hashCode)

  val Netty3: TransportImpl = TransportImpl(params => Netty3Transporter(MysqlClientPipelineFactory, _, params))

  val Netty4: TransportImpl = TransportImpl { params =>
    { addr =>
      new Transporter[Packet, Packet] {
        private[this] val bufTransporter =
          Netty4Transporter.framedBuf(Some(framerFactory), addr, params)

        def apply(): Future[Transport[Packet, Packet]] = {
          bufTransporter().map { bufTransport =>
            bufTransport.map(_.toBuf, Packet.fromBuf)
          }
        }

        def remoteAddress: SocketAddress = bufTransporter.remoteAddress

        // Used in the registry
        override def toString: String = bufTransporter.toString
      }
    }
  }

  implicit val param: Stack.Param[TransportImpl] = Stack.Param(
    if (useNetty4) Netty4
    else Netty3
  )

  private val framerFactory = () => {
    new LengthFieldFramer(
      lengthFieldBegin = 0,
      lengthFieldLength = 3,
      lengthAdjust = Packet.HeaderSize, // Packet size field doesn't include the header size.
      maxFrameLength = Packet.HeaderSize + Packet.MaxBodySize,
      bigEndian = false
    )
  }
}

case class TransportImpl(
    transporter: Stack.Params => SocketAddress => Transporter[Packet, Packet]) {
  def mk(): (TransportImpl, Stack.Param[TransportImpl]) = {
    (this, TransportImpl.param)
  }
}
