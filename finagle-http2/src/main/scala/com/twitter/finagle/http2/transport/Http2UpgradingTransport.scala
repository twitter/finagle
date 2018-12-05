package com.twitter.finagle.http2.transport

import com.twitter.finagle.{FailureFlags, Stack, Status}
import com.twitter.finagle.http2.RefTransport
import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.{Future, Promise, Return, Throw, Time}
import scala.util.control.NoStackTrace

/**
 * This transport waits for a message that the upgrade has either succeeded or
 * failed when it reads.  Once it learns of one of the two, it changes `ref` to
 * respect that upgrade.  Since `ref` starts out pointing to
 * `Http2UpgradingTransport`, once it updates `ref`, it knows it will no longer
 * take calls to write or read.
 *
 * It's possible for a call to `close` to race with the transport upgrade.
 * However, either outcome of the race is OK. The call to `super.close` ensures
 * resources are cleaned up appropriately regardless of the stage of the upgrade.
 */
private[http2] final class Http2UpgradingTransport(
  underlying: Transport[Any, Any],
  ref: RefTransport[Any, Any],
  p: Promise[Option[ClientSession]],
  params: Stack.Params,
  http1Status: () => Status)
    extends TransportProxy[Any, Any](underlying) {

  import Http2UpgradingTransport._

  @volatile private[this] var upgradeFailed = false

  def write(any: Any): Future[Unit] = underlying.write(any)

  // If we failed the upgrade, we want to mark ourselves closed once the parent transporter
  // has generated an H2 session so that we can converge to H2 sessions, if possible.
  override def status: Status = {
    if (upgradeFailed) Status.worst(super.status, http1Status())
    else super.status
  }

  private[this] def upgradeRejected(): Unit = {
    upgradeFailed = true
    p.updateIfEmpty(Return.None)
    // we need ref to update before we can read again
    ref.update(identity)
  }

  private[this] def upgradeIgnored(): Unit = {
    upgradeFailed = true
    p.updateIfEmpty(Throw(UpgradeIgnoredException))
  }

  private[this] def upgradeSuccessful(
    session: ClientSession,
    firstStream: Transport[Any, Any]
  ): Unit = {
    // This removes us from the transport pathway
    ref.update { _ =>
      firstStream
    }

    // Let the `Http2Transporter` know about the shiny new h2 session.
    // We need to do this *after* taking the first stream.
    p.updateIfEmpty(Return(Some(session)))
  }

  def read(): Future[Any] = underlying.read().flatMap {
    case UpgradeSuccessful(f) =>
      val (session, first) = f(underlying)
      upgradeSuccessful(session, first)
      ref.read()
    case UpgradeRejected =>
      upgradeRejected()
      ref.read()
    case UpgradeAborted =>
      upgradeIgnored()
      ref.read()
    case result =>
      Future.value(result)
  }

  override def close(deadline: Time): Future[Unit] = {
    if (p.updateIfEmpty(Throw(new Http2UpgradingTransport.ClosedWhileUpgradingException()))) {
      val statsReceiver = params[Stats].statsReceiver
      statsReceiver.counter("closed_before_upgrade").incr()
    }
    super.close(deadline)
  }
}

private[http2] object Http2UpgradingTransport {

  sealed trait UpgradeResult extends Product with Serializable

  /**
   * Signals that the h2c pipeline was upgraded.
   *
   * TODO: once we remove the old H2 client impl we should not have UpgradeSuccessful carrying a lambda
   * The lambda carries the logic necessary to generate a `ClientSession` and the first `Transport`
   * that represents the stream of the upgrade request.
   */
  case class UpgradeSuccessful(
    makeSession: Transport[Any, Any] => (ClientSession, Transport[Any, Any]))
      extends UpgradeResult

  /** Signals that the h2c upgrade was rejected or ignored by the peer. */
  case object UpgradeRejected extends UpgradeResult

  /** Signals that client refused to attempt an upgrade due to the nature of the request. */
  case object UpgradeAborted extends UpgradeResult

  class ClosedWhileUpgradingException(val flags: Long = FailureFlags.Empty)
      extends Exception("h2c transport was closed while upgrading")
      with HasLogLevel
      with FailureFlags[ClosedWhileUpgradingException] {

    def logLevel: Level = Level.DEBUG // this happens often on interrupts, so let's be quiet
    protected def copyWithFlags(newFlags: Long): ClosedWhileUpgradingException =
      new ClosedWhileUpgradingException(newFlags)
  }

  object UpgradeIgnoredException
      extends Exception("Upgrade not attempted")
      with HasLogLevel
      with NoStackTrace {
    override def logLevel: Level = Level.DEBUG
  }
}
