package com.twitter.finagle.http2.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.DeadTransport
import com.twitter.finagle.http2.transport.Http2ClientDowngrader.StreamMessage
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.finagle.param.{Stats, Timer => TimerParam}
import com.twitter.finagle.{Stack, Status}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Return, Time}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference

/**
 * This `Transporter` makes `Transports` that speak netty http/1.1, but writes
 * http/2 to the wire.  It also caches a connection per address so that it can
 * be multiplexed under the hood.
 *
 * It doesn't attempt to do an http/1.1 upgrade, and has no ability to downgrade
 * to http/1.1 over the wire if the remote server doesn't speak http/2.
 * Instead, it speaks http/2 from birth.
 */
private[http2] final class PriorKnowledgeTransporter(
  underlying: Transporter[Any, Any, TransportContext],
  params: Stack.Params
) extends Transporter[Any, Any, TransportContext] with MultiplexTransporter { self =>

  private[this] val log = Logger.get()
  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")
  private[this] val timer = params[TimerParam].timer
  private[this] val cachedSession = new AtomicReference[Option[Future[StreamTransportFactory]]](None)

  def remoteAddress: SocketAddress = underlying.remoteAddress

  private[this] def createStreamTransportFactory(): Future[StreamTransportFactory] = {
    underlying().map { transport =>
      val inOutCasted = Transport.cast[StreamMessage, StreamMessage](transport)
      val contextCasted = inOutCasted.asInstanceOf[
        Transport[StreamMessage, StreamMessage] {
          type Context = TransportContext with HasExecutor
        }
      ]
      val streamFac = new StreamTransportFactory(
        contextCasted,
        remoteAddress,
        params
      )
      transport.onClose.ensure {
        cachedSession.set(None)
      }

      // Consider the creation of a new prior knowledge transport as an upgrade
      upgradeCounter.incr()
      streamFac
    }
  }

  private[this] def getStreamTransportFactory(): Future[StreamTransportFactory] = {
    cachedSession.get match {
      case Some(f) => f
      case None =>
        val p = Promise[StreamTransportFactory]()
        if (cachedSession.compareAndSet(None, Some(p))) {
          p.become(createStreamTransportFactory())
          p
        } else getStreamTransportFactory()
    }
  }

  def apply(): Future[Transport[Any, Any]] = getStreamTransportFactory().flatMap { fac =>
    fac.newChildTransport().handle {
      case exn: Throwable =>
        log.warning(
          exn,
          s"A previously successful connection to address $remoteAddress stopped being successful."
        )

        cachedSession.set(None)
        new DeadTransport(exn, remoteAddress)
    }
  }

  def close(deadline: Time): Future[Unit] = cachedSession.get match {
    case None => Future.Done
    case Some(f) =>
      f.flatMap(_.close(deadline))
        .by(timer, deadline)
        .rescue { case _: Throwable => Future.Done }
  }

  def sessionStatus: Status = cachedSession.get match {
    case None => Status.Open
    case Some(session) =>
      session.poll match {
        case Some(Return(session)) => session.status
        case _ =>
          // this state is transient, either we're going to kill a dead session soon or
          // we haven't resolved a session yet.
          Status.Open
      }
  }
}
