package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicReference
import java.util.logging.{Level, Logger}
import com.twitter.finagle.{ClientConnection, CodecException, Service, WriteTimedOutException}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.service.ProxyService
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.util.{Future, Promise, Return, Throw}
import org.jboss.netty.channel._
import org.jboss.netty.handler.timeout.ReadTimeoutException

private[finagle] object ServiceToChannelHandler {
  // valid transitions are:
  //
  //    Idle <=> Busy
  //    {Idle, Busy, Draining} => {Draining, Shutdown}
  object State extends Enumeration {
    type State = Value
    val Idle, Busy, Draining, Shutdown = Value
  }
}

private[finagle] class ServiceToChannelHandler[Req, Rep](
    service: Service[Req, Rep],
    postponedService: Promise[Service[Req, Rep]],
    serviceFactory: (ClientConnection) => Service[Req, Rep],
    statsReceiver: StatsReceiver,
    log: Logger)
  extends ChannelClosingHandler with ConnectionLifecycleHandler
{
  import ServiceToChannelHandler._
  import State._

  private[this] val state = new AtomicReference[State](Idle)
  private[this] val onShutdownPromise = new Promise[Unit]

  // we know there's only one outstanding request at a time because ServerBuilder adds it in a separate layer.
  @volatile private[this] var currentResponse: Option[Future[Rep]] = None

  private[this] def shutdown() =
    if (state.getAndSet(Shutdown) != Shutdown) {
      currentResponse foreach { _.cancel() }
      currentResponse = None
      close() onSuccessOrFailure { onShutdownPromise() = Return(()) }
      service.release()
    }

  /**
   * onShutdown: this Future is satisfied when the channel has been
   * closed.
   */
  val onShutdown: Future[Unit] = onShutdownPromise

  /**
   * drain(): admit no new requests.
   */
  def drain() = {
    var continue = false
    do {
      continue = false
      if      (state.compareAndSet(Idle, Draining)) shutdown()
      else if (state.compareAndSet(Busy, Draining)) ()
      else if (state.get == Shutdown)               ()
      else if (state.get == Draining)               ()
      else continue = true
    } while (continue)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    val channel = ctx.getChannel
    val message = e.getMessage

    var oldState: State = null
    do {
      val state_ = state.get
      if (state_ != Idle)
        oldState = state_
      if (state.compareAndSet(Idle, Busy))
        oldState = Idle
    } while (oldState eq null)

    oldState match {
      case Idle => ()
      case Busy => throw new CodecException("Codec issued concurrent requests")
      case _    => /* let these fall on the floor */ return
    }

    try {
      val promise = service(message.asInstanceOf[Req]) respond {
        case Return(value) =>
          currentResponse = None
          Channels.write(channel, value)
        case Throw(e: Throwable) =>
          currentResponse = None
          log.log(Level.WARNING, "service exception", e)
          shutdown()
      }
      currentResponse = Some(promise)
      promise
    } catch {
      case e: ClassCastException =>
        log.log(
          Level.SEVERE,
          "Got ClassCastException while processing a " +
          "message. This is a codec bug. %s".format(e))

        shutdown()

      case e =>
        Channels.fireExceptionCaught(channel, e)
    }
  }

  protected def channelConnected(ctx: ChannelHandlerContext, onClose: Future[Unit]) {
    val channel = ctx.getChannel
    val clientConnection = new ClientConnection {
      def remoteAddress = channel.getRemoteAddress
      def localAddress = channel.getLocalAddress
      def close() { channel.disconnect() }
    }
    postponedService.setValue(serviceFactory(clientConnection))
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    shutdown()
  }

  override def writeRequested(ctx: ChannelHandlerContext, e: MessageEvent) {
    /**
     * This is here and not on Channels.write's return value because
     * there is a race where the future is complete before this
     * callback is added and then the state is out of date. We need to
     * have this callback added BEFORE the NioWorker has a chance to
     * complete it, otherwise we run the risk of receiving more
     * messages before the callback runs.
     */
    e.getFuture onSuccessOrFailure {
      val becameIdle = state.compareAndSet(Busy, Idle)
      if (!becameIdle && state.get == Draining) shutdown()
    }
    super.writeRequested(ctx, e)
  }

  /**
   * Catch and silence certain closed channel exceptions to avoid spamming
   * the logger.
   */
  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
    val cause = e.getCause
    val level = cause match {
      case e: java.nio.channels.ClosedChannelException =>
        Level.FINEST
      case e: ReadTimeoutException =>
        statsReceiver.counter("read_timeout").incr()
        Level.FINEST
      case e: WriteTimedOutException =>
        statsReceiver.counter("write_timeout").incr()
        Level.FINEST
      case e: java.io.IOException
      if (e.getMessage == "Connection reset by peer" ||
          e.getMessage == "Broken pipe") =>
        Level.FINEST
      case e: Throwable =>
        Level.WARNING
    }

    log.log(level, "Exception caught by service channel handler", cause)

    shutdown()
  }
}
