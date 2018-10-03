package com.twitter.finagle

import com.twitter.io.Buf

package object decoder {

  /**
   * A `Decoder` performs protocol decoding. As `Buf`s arrive on the wire, a
   * decoder accumulates them until completed messages arrive. The return value
   * is an ordered sequence of any completed messages as a result of accumulating
   * the additional Buf. If no complete messages are present, an empty collection is
   * returned.
   * Stateful implementations should be expected.
   */
  private[twitter] type Decoder[T] = (Buf => IndexedSeq[T])

  /**
   * A `Framer` performs protocol framing. As `Buf`s arrive on the wire, a
   * framer accumulates them until completed frames arrive. The return value
   * is an ordered sequence of any completed frames as a result of accumulating
   * the additional Buf. If no complete frames are present, an empty collection is
   * returned.
   * Stateful implementations should be expected.
   *
   * @see [[LengthFieldFramer]] as an example
   *      implementation.
   */
  private[twitter] type Framer = Decoder[Buf]
}
