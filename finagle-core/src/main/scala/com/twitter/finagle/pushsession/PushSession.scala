package com.twitter.finagle.pushsession

import com.twitter.finagle.Status
import com.twitter.util.Closable

/**
 * Representation of a push-based protocol session.
 *
 * The [[PushSession]] is intended to be used with the [[PushChannelHandle]]
 * abstraction to provide the interface for building a push-based protocol
 * implementation. In this pattern, events coming from the socket are
 * 'pushed' into the session via the `receive` method with well defined
 * thread behavior. Specifically, the `receive` method will be called with
 * new events from the single-threaded `Executor` available in the
 * associated [[PushChannelHandle]]. This provides two key benefits for push-based
 * protocol implementations:
 * - We remove the overhead of the `Future` abstraction intrinsic to the
 *   `Transport` and `Dispatcher` based model.
 * - The session itself provides a clear pattern for managing synchronization
 *   that works well with the `Promise` abstraction by avoiding explicit
 *   synchronization. See the `README.md` for more details.
 */
abstract class PushSession[In, Out](handle: PushChannelHandle[In, Out]) extends Closable {

  /**
   * Receive a message, most commonly from the socket. Received messages are considered
   * owned by the [[PushSession]] meaning the session is responsible for cleaning up any
   * associated resources.
   */
  def receive(message: In): Unit

  /**
   * The current availability [[Status]] of this [[PushSession]].
   */
  def status: Status
}
