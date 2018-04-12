package com.twitter.finagle.http2.transport

import com.twitter.finagle.{FailureFlags, Stack, Status}
import com.twitter.finagle.http2.{Http2Transporter, RefTransport}
import com.twitter.finagle.http2.transport.Http2UpgradingTransport.UpgradeIgnoredException
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.finagle.transport.{Transport, TransportContext, TransportProxy}
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.{Future, Promise, Return, Throw, Time}
import scala.util.control.NoStackTrace

/**
 * This transport waits for a message that the upgrade has either succeeded or
 * failed when it reads.  Once it learns of one of the two, it changes `ref` to
 * respect that upgrade.  Since `ref` starts out pointing to
 * `Http2UpgradingTransport`, once it updates `ref`, it knows it will no longer
 * take calls to write or read.
 */
private[http2] final class Http2UpgradingTransport(
  t: Transport[Any, Any],
  ref: RefTransport[Any, Any],
  p: Promise[Option[StreamTransportFactory]],
  params: Stack.Params,
  http1Status: () => Status
) extends TransportProxy[Any, Any](t) {

  import Http2Transporter._
  import Http2ClientDowngrader.StreamMessage

  @volatile private[this] var upgradeFailed = false

  def write(any: Any): Future[Unit] = t.write(any)

  // If we failed the upgrade, we want to mark ourselves closed once the parent transporter
  // has generated an H2 session so that we can converge to H2 sessions, if possible.
  override def status: Status = {
    if (upgradeFailed) Status.worst(super.status, http1Status())
    else super.status
  }

  private[this] def upgradeRejected(): Unit = synchronized {
    upgradeFailed = true
    p.updateIfEmpty(Return.None)
    // we need ref to update before we can read again
    ref.update(identity)
  }

  private[this] def upgradeIgnored(): Unit = synchronized {
    upgradeFailed = true
    p.updateIfEmpty(Throw(UpgradeIgnoredException))
  }

  private[this] def upgradeSuccessful(): Unit = synchronized {
    val inOutCasted = Transport.cast[StreamMessage, StreamMessage](t)
    val contextCasted = inOutCasted.asInstanceOf[
      Transport[StreamMessage, StreamMessage] {
        type Context = TransportContext with HasExecutor
      }
    ]
    val fac = new StreamTransportFactory(contextCasted, t.remoteAddress, params)
    // This removes us from the transport pathway
    ref.update { _ =>
      unsafeCast(fac.first())
    }

    // Let the `Http2Transporter` know about the shiny new h2 session.
    // We need to do this *after* taking the first stream.
    p.updateIfEmpty(Return(Some(fac)))
  }

  def read(): Future[Any] = t.read().flatMap {
    case UpgradeRequestHandler.UpgradeRejected =>
      upgradeRejected()
      ref.read()
    case UpgradeRequestHandler.UpgradeSuccessful =>
      upgradeSuccessful()
      ref.read()
    case UpgradeRequestHandler.UpgradeAborted =>
      upgradeIgnored()
      ref.read()
    case result =>
      Future.value(result)
  }

  override def close(deadline: Time): Future[Unit] = synchronized {
    p.updateIfEmpty(Throw(new Http2UpgradingTransport.ClosedWhileUpgradingException()))
    super.close(deadline)
  }
}

private object Http2UpgradingTransport {
  class ClosedWhileUpgradingException(private[finagle] val flags: Long = FailureFlags.Empty)
      extends Exception("h2c transport was closed while upgrading")
      with HasLogLevel
      with FailureFlags[ClosedWhileUpgradingException] {

    def logLevel: Level = Level.DEBUG // this happens often on interrupts, so let's be quiet
    protected def copyWithFlags(newFlags: Long): ClosedWhileUpgradingException =
      new ClosedWhileUpgradingException(newFlags)
  }

  object UpgradeIgnoredException extends Exception("Upgrade not attempted") with HasLogLevel with NoStackTrace {
    override def logLevel: Level = Level.DEBUG
  }
}
