package com.twitter.finagle.pushsession

import com.twitter.util.Future

/**
 * PushSessionTransporters attempt to construct a [[PushChannelHandle]]
 * and provide it to the factory function, returning any errors as
 * failed `Future`s.
 *
 * @note There is one [[PushTransporter]] assigned per remote peer.
 */
trait PushTransporter[In, Out] {

  /**
   * Build a new connection with the `builder` function. The returned
   * `Future[T]` will not resolve until after the `Future[T]` generated
   * by `builder` has resolved, which is the same time which channel
   * events will begin to happen.
   */
  def apply[T <: PushSession[In, Out]](builder: PushChannelHandle[In, Out] => Future[T]): Future[T]
}
