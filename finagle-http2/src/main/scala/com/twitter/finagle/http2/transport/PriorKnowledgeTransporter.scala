package com.twitter.finagle.http2.transport

import com.twitter.finagle.client.Transporter
import com.twitter.finagle.http2.DeadTransport
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.finagle.param.{Stats, Timer => TimerParam}
import com.twitter.finagle.{Stack, Status}
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.logging.Logger
import com.twitter.util.{Future, Promise, Return, Time}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

private[http2] object PriorKnowledgeTransporter {
  val log: Logger = Logger.get()
}

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
  params: Stack.Params)
    extends Transporter[Any, Any, TransportContext]
    with MultiplexTransporter { self =>

  import PriorKnowledgeTransporter._

  private[this] val Stats(statsReceiver) = params[Stats]
  private[this] val upgradeCounter = statsReceiver.scope("upgrade").counter("success")
  private[this] val timer = params[TimerParam].timer
  private[this] val cachedSession =
    new AtomicReference[Option[Future[StreamTransportFactory]]](None)

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

      // Consider the creation of a new prior knowledge transport as an upgrade
      upgradeCounter.incr()
      streamFac
    }
  }

  // We're returning `Some(_)` intentionally as we need the exact reference for the CAS
  // later. AtomicReference checks reference equality, not value equality.
  //
  // This method is recursive as we end up trying to grab an instance of StreamTransportFactory
  // again when some other thread raced with us.
  @tailrec
  private[this] def getStreamTransportFactory(): Some[Future[StreamTransportFactory]] =
    cachedSession.get match {
      case f @ Some(_) => f
      case None =>
        val p = new Promise[StreamTransportFactory]
        val f = Some(p)
        if (cachedSession.compareAndSet(None, f)) {
          p.become(createStreamTransportFactory())
          f
        } else getStreamTransportFactory()
    }

  def apply(): Future[Transport[Any, Any]] = {
    val result = getStreamTransportFactory()

    result.x.flatMap { fac =>
      fac.newChildTransport().rescue {
        case exn: Throwable =>
          // We try to evict the current session concurrently. We recurse if some other thread raced
          // with us.
          if (cachedSession.compareAndSet(result, None)) {
            log.warning(
              exn,
              s"A previously successful connection to address $remoteAddress stopped being successful."
            )

            // We rely on other machinery to close failed transport (ping detector, etc).
            Future.value(new DeadTransport(exn, remoteAddress))
          } else apply()
      }
    }
  }

  @tailrec
  def close(deadline: Time): Future[Unit] = cachedSession.get match {
    case None => Future.Done
    case prev @ Some(f) =>
      if (cachedSession.compareAndSet(prev, None)) {
        f.flatMap(_.close(deadline))
          .by(timer, deadline)
          .rescue { case _: Throwable => Future.Done }
      } else close(deadline)
  }

  def transporterStatus: Status = cachedSession.get match {
    case None => Status.Open
    case Some(session) =>
      session.poll match {
        case Some(Return(session)) if session.status == Status.Busy => Status.Busy
        case _ =>
          // this state is transient, either we're going to kill a dead session soon or
          // we haven't resolved a session yet.
          Status.Open
      }
  }
}
