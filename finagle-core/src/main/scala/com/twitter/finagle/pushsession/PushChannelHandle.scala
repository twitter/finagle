package com.twitter.finagle.pushsession

import com.twitter.finagle.{ClientConnection, Status}
import com.twitter.finagle.ssl.session.SslSessionInfo
import com.twitter.util.{Closable, Try}
import java.util.concurrent.Executor

/**
 * Abstraction for interacting with the underlying I/O pipeline.
 *
 * The `ChannelHandle` provides tools for writing messages to the peer, an `Executor` which
 * provides single threaded behavior for executed tasks, and information about the peer and
 * state of the pipeline.
 *
 * All method calls on the `ChannelHandle` are guaranteed not to result in re-entrance into
 * the [[PushSession]] so long as these methods are called from within the `serialExecutor`.
 * Specifically, if a session invokes a method on the handle it will not result in a new event
 * reaching the session before the method call has returned. This avoids situations such as a
 * session performing a write and before the call returns a new inbound message arrives and
 * mutates session state in an unexpected way.
 *
 * All failures are fatal to the [[PushChannelHandle]] including write failures. Specifically,
 * any failure results in the `onClose` promise being completed with the exception in the
 * `Throw` pathway and the underlying socket will be closed.
 */
trait PushChannelHandle[In, Out] extends Closable with ClientConnection {

  /**
   * Single threaded executor meaning that every computation is run sequentially and provides
   * a happens-before relationship with respect to every other computation executed by the
   * Executor. Messages from the pipeline are guaranteed to be processed in this `Executor`.
   */
  def serialExecutor: Executor

  /**
   * Replaces the current `PushSession`, directing inbound events to the new session.
   *
   * @note It is unsafe to register a new session from outside of serial executor and to
   *       do so will result in undefined behavior.
   *
   * @note other than no longer receiving inbound messages, the previous session is still
   *       active and it is the responsibility of the caller to release any resources held
   *       by the previous session.
   */
  def registerSession(newSession: PushSession[In, Out]): Unit

  /**
   * Write multiple messages to the underlying IO pipeline.
   *
   * @note the provided continuation is guaranteed to be executed later, meaning
   *       that this method call will return before `onComplete` is run.
   */
  def send(messages: Iterable[Out])(onComplete: Try[Unit] => Unit): Unit

  /**
   * Write a message to the underlying IO pipeline.
   *
   * @note the provided continuation is guaranteed to be executed later, meaning
   *       that this method call will return before `onComplete` is run.
   */
  def send(message: Out)(onComplete: Try[Unit] => Unit): Unit

  /**
   * Write a message to the underlying IO pipeline.
   *
   * Any errors in writing result in closing the pipeline and are propagated
   * through the `onClose` Future (provided it has not yet resolved).
   */
  def sendAndForget(message: Out): Unit

  /**
   * Write a collection of messages to the underlying IO pipeline.
   *
   * Any errors in writing result in closing the pipeline and are propagated through
   * the `onClose` Future (provided it has not yet resolved).
   */
  def sendAndForget(messages: Iterable[Out]): Unit

  /**
   * SSL/TLS session information associated with the push channel handle.
   *
   * @note If SSL/TLS is not being used a `NullSslSessionInfo` will be returned instead.
   */
  def sslSessionInfo: SslSessionInfo

  /**
   * The status of this transport; see [[com.twitter.finagle.Status]] for
   * status definitions.
   */
  def status: Status
}
