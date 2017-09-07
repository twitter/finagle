package com.twitter.finagle.netty4.transport

import com.twitter.finagle.Status
import com.twitter.finagle.transport.{TransportContext, Transport}
import com.twitter.util.{Future, Time}
import java.net.SocketAddress
import java.security.cert.Certificate
import java.util.concurrent.Executor

private[finagle] class Netty4Context(underlying: Transport[_, _], val executor: Executor)
    extends TransportContext
    with HasExecutor {
  def status: Status = underlying.status
  def close(deadline: Time): Future[Unit] = underlying.close(deadline)
  def onClose: Future[Throwable] = underlying.onClose
  def localAddress: SocketAddress = underlying.localAddress
  def remoteAddress: SocketAddress = underlying.remoteAddress
  def peerCertificate: Option[Certificate] = underlying.peerCertificate
}
