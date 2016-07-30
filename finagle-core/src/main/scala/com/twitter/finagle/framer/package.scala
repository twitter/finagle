package com.twitter.finagle

import com.twitter.io.Buf

package object framer {

  /**
   * A `Framer` performs protocol framing. As `Buf`s arrive on the wire, a
   * framer accumulates them until completed frames arrive. The return value
   * is an ordered sequence of any completed frames as a result of accumulating
   * the additional Buf. If no complete frames are present, an empty list is
   * returned.
   * Stateful implementations should be expected.
   * @see [[com.twitter.finagle.framer.LengthFieldFramer]] as an example
   *      implementation.
   */
  type Framer = (Buf => IndexedSeq[Buf])
}
