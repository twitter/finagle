package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.Status
import com.twitter.finagle.Status.Closed
import com.twitter.finagle.http.Message
import com.twitter.finagle.http.Multi
import com.twitter.finagle.http.StreamTransport
import com.twitter.finagle.http.StreamTransportProxy
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Try
import scala.util.control.NonFatal

/**
 * A Transport with close behavior suitable for single-use H2 pipelines.
 *
 * In the Netty MultiplexCodec world H2 streams each get their own channel.
 * For now we're representing them each as their own Finagle `Transport` and
 * as such they will all only do 1 dispatch. This allows us to reduce the
 * complexity of the `HttpTransport` significantly to improve performance.
 */
private[finagle] final class Http2Transport[In <: Message, Out <: Message](
  self: StreamTransport[In, Out])
    extends StreamTransportProxy[In, Out](self)
    with (Try[Multi[Out]] => Unit) {

  // We start with 2 half streams (read and write) in the 'open' state. As the
  // read and write close they will decrement by 1, and at 0 both are closed.
  @volatile private[this] var count = 2

  // A respond handler for `read`.
  def apply(mb: Try[Multi[Out]]): Unit = mb match {
    case Return(Multi(m, onFinish)) => observeMessage(m, onFinish)
    case _ => // do nothing
  }

  def read(): Future[Multi[Out]] = self.read().respond(this)

  def write(m: In): Future[Unit] =
    try {
      val f = self.write(m)
      observeMessage(m, f)
      f
    } catch {
      case NonFatal(e) => Future.exception(e)
    }

  override def status: Status = if (count == 0) Closed else self.status

  private[this] def observeMessage(message: Message, onFinish: Future[Unit]): Unit = {
    if (onFinish.isDefined) endHalfStream()
    else
      onFinish.respond { _ => endHalfStream() }
  }

  private[this] def endHalfStream(): Unit = {
    val shouldClose = synchronized {
      val i = count
      if (i == 0) false // already done
      else {
        val next = i - 1
        count = next
        // if we're at 0, it's our job to close things down.
        next == 0
      }
    }

    if (shouldClose) self.close()
  }
}
