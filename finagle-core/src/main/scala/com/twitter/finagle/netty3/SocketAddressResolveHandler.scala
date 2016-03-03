package com.twitter.finagle.netty3

import java.net.{InetAddress, InetSocketAddress, UnknownHostException}

import com.twitter.finagle.InconsistentStateException
import com.twitter.finagle.util.Rng
import org.jboss.netty.channel._

private[finagle] trait SocketAddressResolver extends (String => Either[Throwable, InetAddress])

private[finagle] object SocketAddressResolver {
  val random = new SocketAddressResolver {
    def apply(hostName: String): Either[Throwable, InetAddress] = {
      try {
        // NOTE: Important InetAddress and DNS cache policy
        // InetAddress implementation caches DNS resolutions but doesn't respect DNS TTL responses.
        // To control TTL, we need to configure networkaddress.cache.ttl security property.
        // However, do not completely disable cache. That may cause significant performance issue.
        val addresses = InetAddress.getAllByName(hostName)

        if (addresses.nonEmpty) {
          val index = Rng.threadLocal.nextInt(addresses.length)
          Right(addresses(index))
        } else {
          // Shouldn't reach here.
          Left(new UnknownHostException(hostName))
        }
      } catch {
        case t: Throwable =>
          Left(t)
      }
    }
  }
}

private[finagle] class SocketAddressResolveHandler(
  resolver: SocketAddressResolver,
  addr: InetSocketAddress
) extends SimpleChannelHandler {
  override def connectRequested(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    (e, e.getValue) match {
      case (de: DownstreamChannelStateEvent, socketAddress: InetSocketAddress) if socketAddress.isUnresolved =>
        ctx.getPipeline.execute(new Runnable {
          override def run() {
            resolver(socketAddress.getHostName) match {
              case Right(address) =>
                val resolvedSocketAddress = new InetSocketAddress(address, socketAddress.getPort)
                val resolvedEvent = new DownstreamChannelStateEvent(
                  de.getChannel,
                  de.getFuture,
                  de.getState,
                  resolvedSocketAddress
                )
                SocketAddressResolveHandler.super.connectRequested(ctx, resolvedEvent)
              case Left(t) =>
                de.getFuture.setFailure(t)
                Channels.close(ctx.getChannel)
            }
          }
        })
      case _ =>
        e.getFuture.setFailure(new InconsistentStateException(addr))
        Channels.close(ctx.getChannel)
    }
  }
}
