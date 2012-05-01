package com.twitter.finagle.channel

import com.twitter.finagle.dispatch.{ServerDispatcher, ServerDispatcherFactory}
import com.twitter.finagle.service.ProxyService
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.transport.{ChannelTransport, TransportFactory}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.{ClientConnection, WriteTimedOutException}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.util.{Promise, Monitor, Return}
import java.util.logging.{Logger, Level}
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.ChannelGroup
import org.jboss.netty.handler.timeout.ReadTimeoutException

/**
 * Coordinate the creation of this Server's dispatcher and and its
 * transport; handle exceptions.
 */
class ServiceDispatcher[Req, Rep](
    serviceFactory: ServiceFactory[Req, Rep],
    mkDispatcher: ServerDispatcherFactory[Req, Rep],
    statsReceiver: StatsReceiver,
    log: Logger,
    monitor: Monitor,
    channels: ChannelGroup)
  extends SimpleChannelHandler
{
  @volatile private[this] var clientConnection: ClientConnection = ClientConnection.nil
  @volatile private[this] var dispatcher: ServerDispatcher = ServerDispatcher.nil
  @volatile private[this] var channel: Channel = null

  private[this] def severity(exc: Throwable) = exc match {
    case
        _: java.nio.channels.ClosedChannelException
      | _: javax.net.ssl.SSLException
      | _: ReadTimeoutException
      | _: WriteTimedOutException
      | _: javax.net.ssl.SSLException => Level.FINEST
    case e: java.io.IOException if (
      e.getMessage == "Connection reset by peer" ||
      e.getMessage == "Broken pipe" ||
      e.getMessage == "Connection timed out" ||
      e.getMessage == "No route to host"
    ) => Level.FINEST
    case _ => Level.WARNING
  }

  def drain(): Unit = dispatcher.drain()

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    channel = e.getChannel
    channels.add(channel)

    clientConnection = new ClientConnection {
      def remoteAddress = channel.getRemoteAddress
      def localAddress = channel.getLocalAddress
      def close() { channel.disconnect() }
      val onClose = {
        val p = new Promise[Unit]
        channel.getCloseFuture onSuccessOrFailure { p.updateIfEmpty(Return(())) }
        p
      }
    }

    val service: Service[Req, Rep] = {
      val s = serviceFactory(clientConnection)
      s.poll map { _.apply() } getOrElse (new ProxyService(s))
    }

    // Note that since ServiceDispatcher is added to the original pipeline,
    // we are guaranteed not to drop any messages here.
    val mkTrans = new TransportFactory {
      def apply[In, Out]() = new ChannelTransport[In, Out](channel)
    }

    dispatcher = mkDispatcher(mkTrans, service)

    super.channelOpen(ctx, e)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    val cause = e.getCause
    monitor.handle(cause)

    cause match {
      case e: ReadTimeoutException =>
        statsReceiver.counter("read_timeout").incr()
      case e: WriteTimedOutException =>
        statsReceiver.counter("write_timeout").incr()
      case _ =>
        ()
    }

    val msg = "Unhandled exception in connection with " +
      clientConnection.remoteAddress.toString +
      " , shutting down connection"

    log.log(severity(cause), msg, cause)
    if (e.getChannel.isOpen)
      Channels.close(e.getChannel)
  }
}
