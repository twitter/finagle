package com.twitter.finagle

import com.twitter.io.Buf

package object codec {
  type FrameEncoder[-Out] = (Out => Buf)

  // as can be surmised from the type, stateful implementations
  // of `FrameDecoder` should be expected.
  type FrameDecoder[+In] = (Buf => IndexedSeq[In])
}
