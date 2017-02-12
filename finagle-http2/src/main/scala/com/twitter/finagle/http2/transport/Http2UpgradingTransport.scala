package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.{RefTransport, Http2Transporter}
import com.twitter.finagle.transport.{Transport, TransportProxy}
import com.twitter.util.{Promise, Future, Try}
import io.netty.handler.codec.http.HttpClientUpgradeHandler.UpgradeEvent
import io.netty.handler.codec.http.HttpObject

/**
 * This transport waits for a message that the upgrade has either succeeded or
 * failed when it reads.  Once it learns of one of the two, it changes `ref` to
 * respect that upgrade.  Since `ref` starts out pointing to
 * `Http2UpgradingTransport`, once it updates `ref`, it knows it will no longer
 * take calls to write or read.
 */
private[http2] class Http2UpgradingTransport(
    t: Transport[Any, Any],
    ref: RefTransport[Any, Any],
    p: Promise[Option[() => Try[Transport[HttpObject, HttpObject]]]])
  extends TransportProxy[Any, Any](t) {

  import Http2Transporter._

  def write(any: Any): Future[Unit] = t.write(any)
  def read(): Future[Any] = t.read().flatMap {
    case _@UpgradeEvent.UPGRADE_REJECTED =>
      p.setValue(None)
      // we need ref to update before we can read again
      ref.update(identity)
      ref.read()
    case _@UpgradeEvent.UPGRADE_SUCCESSFUL =>
      val casted =
        Transport.cast[Http2ClientDowngrader.StreamMessage, Http2ClientDowngrader.StreamMessage](t)
      val multiplexed = new MultiplexedTransporter(casted, t.remoteAddress)
      p.setValue(Some(multiplexed))
      ref.update { _ =>
        unsafeCast(multiplexed.first())
      }
      ref.read()
    case result =>
      Future.value(result)
  }
}
