package com.twitter.finagle.channel

import java.util.concurrent.atomic.AtomicReference
import java.util.logging.{Level, Logger}

import org.jboss.netty.channel._
import org.jboss.netty.handler.timeout.ReadTimeoutException

import com.twitter.util.{Future, Promise, Return, Throw, Monitor, Time}

import com.twitter.finagle.{ClientConnection, CodecException, Service, WriteTimedOutException}
import com.twitter.finagle.util.Conversions._
import com.twitter.finagle.service.{ProxyService, NilService}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}

private[finagle] object ServiceToChannelHandler {
  // valid transitions are:
  //
  //    Idle <=> Busy
  //    {Idle, Busy, Draining} => {Draining, Shutdown}
  object State extends Enumeration {
    type State = Value
    val Idle, Busy, Draining, Shutdown = Value
  }

  private[ServiceToChannelHandler] def severity(exc: Throwable) = exc match {
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
}

private[finagle] class ServiceToChannelHandler[Req, Rep](
    serviceFactory: (ClientConnection) => Service[Req, Rep],
    statsReceiver: StatsReceiver,
    log: Logger,
    parentMonitor: Monitor)
  extends ChannelClosingHandler with ConnectionLifecycleHandler
{
  import ServiceToChannelHandler._
  import State._

  private[this] val handleStat = statsReceiver.stat("handletime_us")
  @volatile private[this] var clientConnection: ClientConnection = ClientConnection.nil
  @volatile private[this] var service: Service[Req, Rep] = NilService
  private[this] val state = new AtomicReference[State](Idle)
  private[this] val onShutdownPromise = new Promise[Unit]
  private[this] val monitor =
    parentMonitor andThen Monitor.mk {
      case e =>
        val msg = "Unhandled exception in connection with " +
          clientConnection.remoteAddress.toString +
          " , shutting down connection"

        log.log(severity(e), msg, e)
        shutdown()
        true
    }

  private[this] lazy val serviceMonitor =
    monitor andThen Monitor.mk {
      case exc =>
        log.log(Level.SEVERE, "A Service threw an exception", exc)
        false
    }

  // we know there's only one outstanding request at a time because
  // ServerBuilder adds it in a separate layer.
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

  override def messageReceived(
    ctx: ChannelHandlerContext,
    e: MessageEvent
  ): Unit = {
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
      case Busy =>
        val exc = new CodecException("Codec issued concurrent requests")
        log.log(Level.SEVERE, "Codec error", exc)
        throw exc
      case _    => /* let these fall on the floor */ return
    }

    Future.monitored {
      val begin = Time.now
      val res = service(message.asInstanceOf[Req])
      handleStat.add((Time.now - begin).inMicroseconds)
      currentResponse = Some(res)
      res
    } onSuccess { value =>
      currentResponse = None
      Channels.write(channel, value)
    } onFailure { exc =>
      serviceMonitor.handle(exc)
    }
  }

  protected def channelConnected(ctx: ChannelHandlerContext, _onClose: Future[Unit]) {
    val channel = ctx.getChannel
    clientConnection = new ClientConnection {
      def remoteAddress = channel.getRemoteAddress
      def localAddress = channel.getLocalAddress
      def close() { channel.disconnect() }
      val onClose = _onClose
    }

    service = serviceFactory(clientConnection)
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
   * Hand the exception to our monitor.
   */
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
  }
}
