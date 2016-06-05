package com.twitter.finagle.http.exp

import com.twitter.finagle.Status
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Time}
import java.net.SocketAddress
import java.security.cert.Certificate

/**
 * A multi-part object with a single read handle, and a future that is satisfied
 * when the handle is fully materialized.
 */
private[finagle] case class Multi[A](readHandle: A, onFinish: Future[Unit])

/**
 * A stream transport bridges the idea that a single object might represent
 * multiple underlying streamed pieces, and we may want to track both the start
 * and the finish.
 */
private[finagle] trait StreamTransport[In, Out] extends Transport[In, Multi[Out]] {

  /**
   * Writes a multipart object to the wire, where the [[Future]] is satisfied
   * once the transport has finished writing every part of the object to the
   * wire.
   */
  def write(in: In): Future[Unit]

  /**
   * Reads a multipart object off the wire, where the outer [[Future]] is
   * satisfied once the transport has read enough to expose a handle that can be
   * read off of, and the inner [[Future]] is satisfied once the read handle has
   * finished writing the entire stream off the wire.
   */
  def read(): Future[Multi[Out]]
}

private[finagle] abstract class StreamTransportProxy[In, Out](self: Transport[_, _])
  extends StreamTransport[In, Out] {

  def status: Status = self.status
  val onClose: Future[Throwable] = self.onClose
  def localAddress: SocketAddress = self.localAddress
  def remoteAddress: SocketAddress = self.remoteAddress
  def close(deadline: Time): Future[Unit] = self.close(deadline)
  def peerCertificate: Option[Certificate] = self.peerCertificate
}
