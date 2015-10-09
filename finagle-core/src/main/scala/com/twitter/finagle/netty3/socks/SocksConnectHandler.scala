package com.twitter.finagle.netty3.socks

import com.twitter.finagle.{ChannelClosedException, ConnectionFailedException, InconsistentStateException}
import com.twitter.finagle.socks.{AuthenticationSetting, Unauthenticated, UsernamePassAuthenticationSetting}
import com.twitter.io.Charsets
import java.net.{Inet4Address, Inet6Address, InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicReference
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._

object SocksConnectHandler {
  // Throwables used as `cause` fields for ConnectionFailedExceptions.
  private[socks] val InvalidInit = new Throwable("unexpected SOCKS version or authentication " +
    "level specified in connect response from proxy")

  private[socks] val InvalidResponse = new Throwable("unexpected SOCKS version or response " +
    "status specified in connect response from proxy")

  // Socks Version constants
  private val Version1: Byte = 0x01
  private val Version5: Byte = 0x05

  // Socks IP Address Constants
  private val IpV4Indicator: Byte = 0x01
  private val IpV6Indicator: Byte = 0x04
  private val HostnameIndicator: Byte = 0x03

  // Socks communication constants
  private val Connect: Byte = 0x01
  private val Reserved: Byte = 0x00
  private val SuccessResponse: Byte = 0x00
}

/**
 * Handle connections through a SOCKS proxy.
 *
 * See http://www.ietf.org/rfc/rfc1928.txt
 *
 * Only username and password authentication is implemented;
 * See https://tools.ietf.org/rfc/rfc1929.txt
 *
 * We assume the proxy is provided by ssh -D.
 */
class SocksConnectHandler(
  proxyAddr: SocketAddress,
  addr: InetSocketAddress,
  authenticationSettings: Seq[AuthenticationSetting] = Seq(Unauthenticated))
    extends SimpleChannelHandler {

  import SocksConnectHandler._

  object State extends Enumeration {
    val Start, Connected, Requested, Authenticating = Value
  }

  import State._

  private[this] var state = Start
  private[this] val buf = ChannelBuffers.dynamicBuffer()
  private[this] val bytes = new Array[Byte](4)
  private[this] val connectFuture = new AtomicReference[ChannelFuture](null)
  private[this] val authenticationMap =
    authenticationSettings.map { setting => setting.typeByte -> setting }.toMap
  private[this] val supportedTypes = authenticationMap.keys.toArray.sorted

  // following Netty's ReplayingDecoderBuffer, we throw this when we run out of bytes
  object ReplayError extends scala.Error

  private[this] def fail(c: Channel, t: Throwable) {
    Option(connectFuture.get) foreach { _.setFailure(t) }
    Channels.close(c)
  }

  private[this] def write(ctx: ChannelHandlerContext, msg: Any) {
    Channels.write(ctx, Channels.future(ctx.getChannel), msg, null)
  }

  private[this] def writeInit(ctx: ChannelHandlerContext) {
    val buf = ChannelBuffers.dynamicBuffer(1024)
    buf.writeByte(Version5)
    buf.writeByte(supportedTypes.length.toByte)
    buf.writeBytes(supportedTypes)

    write(ctx, buf)
  }

  private[this] def readInit(): Option[AuthenticationSetting] = {
    checkReadableBytes(2)
    buf.readBytes(bytes, 0, 2)
    if (bytes(0) == Version5)
      authenticationMap.get(bytes(1))
    else
      None
  }

  private[this] def writeRequest(ctx: ChannelHandlerContext) {
    val buf = ChannelBuffers.dynamicBuffer(1024)
    buf.writeBytes(Array[Byte](Version5, Connect, Reserved))

    addr.getAddress match {
      case v4Addr: Inet4Address =>
        buf.writeByte(IpV4Indicator)
        buf.writeBytes(v4Addr.getAddress)

      case v6Addr: Inet6Address =>
        buf.writeByte(IpV6Indicator)
        buf.writeBytes(v6Addr.getAddress)

      case _ => // unresolved host
        buf.writeByte(HostnameIndicator)
        val hostnameBytes = addr.getHostName.getBytes(Charsets.UsAscii)
        buf.writeByte(hostnameBytes.length)
        buf.writeBytes(hostnameBytes)
    }

    buf.writeShort(addr.getPort)
    write(ctx, buf)
  }

  private[this]
  def writeUserNameAndPass(ctx: ChannelHandlerContext, username: String, pass: String) {
    val buf = ChannelBuffers.buffer(1024)
    buf.writeByte(Version1)

    // RFC does not specify an encoding. Assume UTF8
    val usernameBytes = username.getBytes(Charsets.Utf8)
    buf.writeByte(usernameBytes.length.toByte)
    buf.writeBytes(usernameBytes)

    val passBytes = pass.getBytes(Charsets.Utf8)
    buf.writeByte(passBytes.length.toByte)
    buf.writeBytes(passBytes)

    write(ctx, buf)
  }

  private[this] def readAuthenticated() = {
    checkReadableBytes(2)
    buf.readBytes(bytes, 0, 2)

    bytes(0) == Version1 && bytes(1) == SuccessResponse
  }

  private[this] def readResponse(): Boolean = {
    checkReadableBytes(4)
    buf.readBytes(bytes, 0, 4)
    if (bytes(0) == Version5 &&
      bytes(1) == SuccessResponse &&
      bytes(2) == Reserved) {
      bytes(3) match {
        case IpV4Indicator =>
          discardBytes(4)

        case HostnameIndicator =>
          checkReadableBytes(1)
          discardBytes(buf.readUnsignedByte())

        case IpV6Indicator =>
          discardBytes(16)
      }
      discardBytes(2)
      true
    } else {
      false
    }
  }

  private[this] def discardBytes(numBytes: Int) {
    checkReadableBytes(numBytes)
    buf.readBytes(numBytes)
  }

  private[this] def checkReadableBytes(numBytes: Int) {
    if (buf.readableBytes < numBytes)
      throw ReplayError
  }

  override def connectRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    e match {
      case de: DownstreamChannelStateEvent =>
        if (!connectFuture.compareAndSet(null, e.getFuture)) {
          fail(ctx.getChannel, new InconsistentStateException(addr))
          return
        }

        // proxy cancellation
        val wrappedConnectFuture = Channels.future(de.getChannel, true)
        de.getFuture.addListener(new ChannelFutureListener {
          def operationComplete(f: ChannelFuture) {
            if (f.isCancelled)
              wrappedConnectFuture.cancel()
          }
        })
        // Proxy failures here so that if the connect fails, it is
        // propagated to the listener, not just on the channel.
        wrappedConnectFuture.addListener(new ChannelFutureListener {
          def operationComplete(f: ChannelFuture) {
            if (f.isSuccess || f.isCancelled)
              return

            fail(f.getChannel, f.getCause)
          }
        })

        val wrappedEvent = new DownstreamChannelStateEvent(
          de.getChannel, wrappedConnectFuture,
          de.getState, proxyAddr)

        super.connectRequested(ctx, wrappedEvent)

      case _ =>
        fail(ctx.getChannel, new InconsistentStateException(addr))
    }
  }

  // we delay propagating connection upstream until we've completed the proxy connection.
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    if (connectFuture.get eq null) {
      fail(ctx.getChannel, new InconsistentStateException(addr))
      return
    }

    // proxy cancellations again.
    connectFuture.get.addListener(new ChannelFutureListener {
      def operationComplete(f: ChannelFuture) {
        if (f.isSuccess)
          SocksConnectHandler.super.channelConnected(ctx, e)
        else if (f.isCancelled)
          fail(ctx.getChannel, new ChannelClosedException(addr))
      }
    })

    state = Connected
    writeInit(ctx)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    if (connectFuture.get eq null) {
      fail(ctx.getChannel, new InconsistentStateException(addr))
      return
    }

    buf.writeBytes(e.getMessage.asInstanceOf[ChannelBuffer])
    buf.markReaderIndex()

    try {
      state match {
        case Connected =>
          readInit() match {
            case Some(Unauthenticated) =>
              state = Requested
              writeRequest(ctx)
            case Some(UsernamePassAuthenticationSetting(username,pass)) =>
              state = Authenticating
              writeUserNameAndPass(ctx, username, pass)
            case None =>
              fail(e.getChannel, new ConnectionFailedException(InvalidInit, addr))
          }

        case Authenticating =>
          if (readAuthenticated()) {
            state = Requested
            writeRequest(ctx)
          } else {
            fail(e.getChannel, new ConnectionFailedException(InvalidResponse, addr))
          }

        case Requested =>
          if (readResponse()) {
            ctx.getPipeline.remove(this)
            connectFuture.get.setSuccess()
          } else {
            fail(e.getChannel, new ConnectionFailedException(InvalidResponse, addr))
          }
      }
    } catch {
      case ReplayError => buf.resetReaderIndex()
    }
  }
}
