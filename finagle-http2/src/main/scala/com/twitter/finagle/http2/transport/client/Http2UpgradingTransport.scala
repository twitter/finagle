package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.param.Stats
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.finagle.{FailureFlags, Stack}
import com.twitter.logging.{HasLogLevel, Level}
import com.twitter.util.{Future, Time}

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
  params: Stack.Params)
    extends TransportProxy[Any, Any](underlying) {

  import Http2UpgradingTransport._

  @volatile private[this] var upgradeProcessComplete = false

  private[this] def noUpgrade(): Unit = {
    upgradeProcessComplete = true
    // This drops the `Http2UpgradingTransport` from the chain so we can avoid
    // the overhead of the `.flatMap` call on every `.read()` operation.
    ref.update(identity)
  }

  private[this] def upgradeSuccessful(firstStream: Transport[Any, Any]): Unit = {
    upgradeProcessComplete = true
    // Redirect to the first HTTP/2 stream for future read calls.
    ref.update(_ => firstStream)
  }

  def write(any: Any): Future[Unit] = underlying.write(any)

  def read(): Future[Any] = underlying.read().flatMap {
    case UpgradeSuccessful(first) =>
      upgradeSuccessful(first)
      ref.read()
    case UpgradeRejected | UpgradeAborted =>
      noUpgrade()
      ref.read()
    case result =>
      Future.value(result)
  }

  override def close(deadline: Time): Future[Unit] = {
    if (!upgradeProcessComplete) {
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
   * @param stream1 The `Transport` that represents the upgrade stream (stream 1)
   *                of the H2 session.
   */
  case class UpgradeSuccessful(stream1: Transport[Any, Any]) extends UpgradeResult

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
}
