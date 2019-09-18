package com.twitter.finagle.mux.transport

/**
 * The level of desire that a peer has to use compression.
 */
sealed abstract class CompressionLevel(val value: String)

object CompressionLevel {

  /**
   * The peer would like to use compression, and will negotiate compression as
   * long as the remote peer indicates it is either Accepted or Desired.
   */
  case object Desired extends CompressionLevel("desired")

  /**
   * The peer is able to use compression, and will negotiate compression as
   * long as the remote peer indicates it is Desired.
   */
  case object Accepted extends CompressionLevel("accepted")

  /**
   * The peer is unable to use compression, and will never negotiate
   * compression.
   */
  case object Off extends CompressionLevel("off")
}
