package com.twitter.finagle.http2.transport

import com.twitter.finagle.{Stack, Status}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.DeadTransport
import com.twitter.finagle.param.{Stats, Timer => TimerParam}
import com.twitter.finagle.stats.Verbosity
import com.twitter.finagle.transport.{Transport, TransportContext, TransportProxy}
import com.twitter.logging.{HasLogLevel, Level, Logger}
import com.twitter.util._
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

/**
 * A foundation for [[Transporter]] implementations that cache an underlying
 * shared session.
 *
 * @param params [[Stack.Params]] used to configure the client.
 * @param underlyingHttp11 A HTTP/1.x [[Transporter]] to use in the case of failure to
 *                         negotiate or for use while an upgrade is pending if
 *                         `waitForSingleton` is `false`.
 * @param fallbackToHttp11WhileNegotiating If `false`, requests sent while negotiation is in
 *                                         progress will wait for the outcome, allowing them
 *                                         to use the same shared session if that is the
 *                                         upgrade result.
 */
private[http2] abstract class Http2NegotiatingTransporter(
  params: Stack.Params,
  underlyingHttp11: Transporter[Any, Any, TransportContext],
  fallbackToHttp11WhileNegotiating: Boolean)
    extends Transporter[Any, Any, TransportContext]
    with MultiplexTransporter {

  import Http2NegotiatingTransporter._

  private[this] val cachedConnection =
    new AtomicReference[Future[Option[ClientSession]]]()

  private[this] val statsReceiver = params[Stats].statsReceiver

  private[this] val cachedSessionClosed = statsReceiver.counter(Verbosity.Debug, "dead_session")
  private[this] val childTransportClosed =
    statsReceiver.counter(Verbosity.Debug, "dead_child_transport")

  /** Attempt to upgrade to a multiplexed session */
  protected def attemptUpgrade(): (Future[Option[ClientSession]], Future[Transport[Any, Any]])

  // Exposed for testing.
  final protected def connectionCached: Boolean = cachedConnection.get != null

  final def remoteAddress: SocketAddress = underlyingHttp11.remoteAddress

  final def close(deadline: Time): Future[Unit] = {
    val f = cachedConnection.getAndSet(null)
    if (f == null) Future.Done
    else {
      val timer = params[TimerParam].timer
      val mapped = f.transform {
        case Return(Some(closable)) => closable.close(deadline)
        case _ => Future.Done
      }
      // TODO: we shouldn't be rescuing a timeout
      mapped.by(timer, deadline).rescue {
        case ex: TimeoutException =>
          mapped.raise(ex)
          Future.Done
      }
    }
  }

  final def transporterStatus: Status = {
    // This reflects the general health of the backend. We want to reflect the busy
    // status of the underlying session since that may reflect health of the backend.
    // However, if it's `Status.Closed` it could be due to normal reasons and our job
    // is to make a new shared session, so this `Transporter` is still `Status.Open`
    // for business.
    val f = cachedConnection.get
    if (f == null) Status.Open
    else
      f.poll match {
        case Some(Return(Some(fac))) if fac.status == Status.Busy => Status.Busy
        case _ => Status.Open
      }
  }

  // We want HTTP/1.x connections to get culled once we have a live HTTP/2 session so
  // we transition their status to `Closed` once we have an H2 session that can be used.
  final protected def http1Status: Status = {
    val f = cachedConnection.get
    if (f == null || (f eq UpgradeRejectedFuture)) Status.Open
    else
      f.poll match {
        case Some(Return(Some(fac))) =>
          fac.status match {
            case Status.Open => Status.Closed
            case status => status
          }
        case _ => Status.Open
      }
  }

  @tailrec
  final def apply(): Future[Transport[Any, Any]] =
    cachedConnection.get match {
      case null =>
        val p = Promise[Option[ClientSession]]()
        if (!cachedConnection.compareAndSet(null, p)) apply()
        else {
          val (futureSession, firstTransport) = attemptUpgrade()
          p.become(futureSession)
          firstTransport
        }

      case futureSession =>
        if (!fallbackToHttp11WhileNegotiating || futureSession.isDefined)
          useExistingConnection(futureSession)
        else fallbackToHttp11() // fall back to http/1.1 while upgrading

    }

  private[this] def tryEvict(f: Future[Option[ClientSession]]): Unit = {
    // kick us out of the cache so we can try to reestablish the connection
    cachedConnection.compareAndSet(f, null)
  }

  private[this] def useExistingConnection(
    f: Future[Option[ClientSession]]
  ): Future[Transport[Any, Any]] = f.transform {
    case Return(Some(session)) if session.status == Status.Closed =>
      log.info("Existing session is closed.")
      cachedSessionClosed.incr()
      tryEvict(f)
      apply()

    case Return(Some(session)) =>
      session.newChildTransport().transform {
        case Return(transport) if transport.status == Status.Closed =>
          log.debug(s"A cached connection to address %s was failed.", remoteAddress)
          childTransportClosed.incr()
          // We evict and give it another go round.
          tryEvict(f)
          apply()

        case Return(transport) => Future.value(transport)
        case Throw(exn) =>
          log.warning(
            exn,
            s"A previously successful connection to address $remoteAddress stopped being successful."
          )
          // We could have failed to acquire a child transport because the session is draining.
          // TODO: who is responsible for calling `close` in this case? Do it close itself?
          tryEvict(f)

          // we expect finagle to treat this specially and retry if possible
          Future.value(deadTransport(exn))
      }

    case Return(None) =>
      // we didn't upgrade
      underlyingHttp11()

    case Throw(exn) =>
      val level = exn match {
        case HasLogLevel(level) => level
        case _ => Level.WARNING
      }
      log.log(level, exn, s"An upgrade attempt to $remoteAddress failed.")
      // We evict and give it another go round.
      tryEvict(f)
      apply()
  }

  // uses http11 underneath, but marks itself as dead if an upgrade succeeds or the connection fails
  private[this] def fallbackToHttp11(): Future[Transport[Any, Any]] = {
    underlyingHttp11().map { http11Trans =>
      new TransportProxy[Any, Any](http11Trans) {
        def write(req: Any): Future[Unit] = http11Trans.write(req)
        def read(): Future[Any] = http11Trans.read()

        override def status: Status =
          Status.worst(http11Trans.status, http1Status)
      }
    }
  }

  private[this] def deadTransport(exn: Throwable) = new DeadTransport(exn, remoteAddress)
}

private object Http2NegotiatingTransporter {
  private val log = Logger.get()

  // Sentinel that we check early to avoid polling a promise forever
  private val UpgradeRejectedFuture: Future[Option[ClientSession]] = Future.None
}
