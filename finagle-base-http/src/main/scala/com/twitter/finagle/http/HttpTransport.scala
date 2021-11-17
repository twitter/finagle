package com.twitter.finagle.http

import com.twitter.finagle.{Status => CoreStatus}
import com.twitter.finagle.http.codec.Http1ConnectionManager
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Try
import scala.util.control.NonFatal

/**
 * A Transport with close behavior managed by ConnectionManager.
 *
 * @note the connection manager will close connections as required by RFC 2616 § 8
 *       irrespective of any pending requests in the dispatcher.
 */
private[finagle] class HttpTransport[A <: Message, B <: Message](
  self: StreamTransport[A, B],
  manager: Http1ConnectionManager)
    extends StreamTransportProxy[A, B](self)
    with (Try[Multi[B]] => Unit) {

  def this(self: StreamTransport[A, B]) =
    this(self, new Http1ConnectionManager)

  // Servers don't use `status` to determine when they should
  // close a transport, so we close the transport when the connection
  // is ready to be closed.
  manager.onClose.before(self.close())

  // A respond handler for `read`.
  def apply(mb: Try[Multi[B]]): Unit = mb match {
    case Return(Multi(m, onFinish)) => manager.observeMessage(m, onFinish)
    case _ => // do nothing
  }

  def read(): Future[Multi[B]] = self.read().respond(this)

  def write(m: A): Future[Unit] =
    try {
      // We have to use an intermediate `Promise` instead of the `Future` from
      // the write call because `.observeMessage` may mutate the message, and
      // thus we could write the wrong message in a race.
      val p = new Promise[Unit]
      manager.observeMessage(m, p)
      val f = self.write(m)
      p.become(f)
      f
    } catch {
      case NonFatal(e) => Future.exception(e)
    }

  override def status: CoreStatus = if (manager.shouldClose) CoreStatus.Closed else self.status
}
