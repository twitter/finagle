package com.twitter.finagle.socks

import java.net.{Inet4Address, Inet6Address, InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.AtomicReference

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.jboss.netty.util.CharsetUtil

import com.twitter.finagle.{ChannelClosedException, ConnectionFailedException, InconsistentStateException}

/**
 * Handle connections through a SOCKS proxy.
 *
 * See http://www.ietf.org/rfc/rfc1928.txt
 *
 * SOCKS authentication is not implemented; we assume the proxy is provided by ssh -D.
 */
class SocksConnectHandler(proxyAddr: SocketAddress, addr: InetSocketAddress)
  extends SimpleChannelHandler
{
  object State extends Enumeration {
    val START, CONNECTED, REQUESTED = Value
  }
  import State._

  private[this] var state = START
  private[this] val buf = ChannelBuffers.dynamicBuffer()
  private[this] val bytes = new Array[Byte](4)
  private[this] val connectFuture = new AtomicReference[ChannelFuture](null)

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
    // 0x05 == version 5
    // 0x01 == 1 authentication method
    // 0x00 == no authentication required
    write(ctx, ChannelBuffers.wrappedBuffer(Array[Byte](0x05, 0x01, 0x00)))
  }

  private[this] def readInit() = {
    checkReadableBytes(2)
    buf.readBytes(bytes, 0, 2)
    // 0x05 == version 5
    // 0x00 == no authentication required
    bytes(0) == 0x05 && bytes(1) == 0x00
  }

  private[this] def writeRequest(ctx: ChannelHandlerContext) {
    val buf = ChannelBuffers.buffer(1024)
    // 0x05 == version 5
    // 0x01 == connect
    // 0x00 == reserved
    buf.writeBytes(Array[Byte](0x05, 0x01, 0x00))

    addr.getAddress match {
      case v4Addr: Inet4Address =>
        // 0x01 == IPv4
        buf.writeByte(0x01)
        buf.writeBytes(v4Addr.getAddress)

      case v6Addr: Inet6Address =>
        // 0x04 == IPv6
        buf.writeByte(0x04)
        buf.writeBytes(v6Addr.getAddress)

      case _ => // unresolved host
        // 0x03 == hostname
        buf.writeByte(0x03)
        val hostnameBytes = addr.getHostName.getBytes(CharsetUtil.US_ASCII)
        buf.writeByte(hostnameBytes.size)
        buf.writeBytes(hostnameBytes)
    }

    buf.writeShort(addr.getPort)
    write(ctx, buf)
  }

  private[this] def readResponse() = {
    checkReadableBytes(4)
    buf.readBytes(bytes, 0, 4)
    // 0x05 == version 5
    // 0x00 == succeeded
    // 0x00 == reserved
    if (bytes(0) == 0x05 && bytes(1) == 0x00 && bytes(2) == 0x00) {
      bytes(3) match {
        case 0x01 => // 0x01 == IPv4
          discardBytes(4)

        case 0x03 => // 0x03 == hostname
          checkReadableBytes(1)
          discardBytes(buf.readUnsignedByte())

        case 0x04 => // 0x04 == IPv6
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
    if (buf.readableBytes < numBytes) {
      throw ReplayError
    }
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

    state = CONNECTED
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
        case CONNECTED =>
          if (readInit()) {
            state = REQUESTED
            writeRequest(ctx)
          } else {
            fail(e.getChannel, new ConnectionFailedException(null, addr))
          }

        case REQUESTED =>
          if (readResponse()) {
            ctx.getPipeline.remove(this)
            connectFuture.get.setSuccess()
          } else {
            fail(e.getChannel, new ConnectionFailedException(null, addr))
          }
      }
    } catch {
      case ReplayError => buf.resetReaderIndex()
    }
  }
}
