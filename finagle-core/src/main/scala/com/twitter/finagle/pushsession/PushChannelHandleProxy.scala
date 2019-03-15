package com.twitter.finagle.pushsession

import com.twitter.finagle.Status
import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.util.{Future, Time, Try}
import java.net.SocketAddress
import java.util.concurrent.Executor

/**
 * Base proxy implementation for [[PushChannelHandle]]
 *
 * Implementations should override methods as appropriate.
 */
abstract class PushChannelHandleProxy[In, Out](underlying: PushChannelHandle[In, Out])
    extends PushChannelHandle[In, Out] {

  def serialExecutor: Executor = underlying.serialExecutor

  def registerSession(newSession: PushSession[In, Out]): Unit =
    underlying.registerSession(newSession)

  def send(messages: Iterable[Out])(onComplete: (Try[Unit]) => Unit): Unit =
    underlying.send(messages)(onComplete)

  def send(message: Out)(onComplete: (Try[Unit]) => Unit): Unit =
    underlying.send(message)(onComplete)

  def sendAndForget(message: Out): Unit = underlying.sendAndForget(message)

  def sendAndForget(messages: Iterable[Out]): Unit = underlying.sendAndForget(messages)

  def sslSessionInfo: SslSessionInfo = underlying.sslSessionInfo

  def status: Status = underlying.status

  def remoteAddress: SocketAddress = underlying.remoteAddress

  def localAddress: SocketAddress = underlying.localAddress

  def onClose: Future[Unit] = underlying.onClose

  def close(deadline: Time): Future[Unit] = underlying.close(deadline)
}
