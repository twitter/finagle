package com.twitter.finagle.toggle

import com.twitter.finagle.toggle.Toggle.Metadata

/**
 * A [[ToggleMap]] implementation where there are no
 * [[Toggle Toggles]].
 */
object NullToggleMap extends ToggleMap {
  override def toString: String = "NullToggleMap"

  def apply(id: String): Toggle = Toggle.Undefined

  def iterator: Iterator[Metadata] = Iterator.empty

  // an optimization that allows for avoiding unnecessary NullToggleMaps
  // by "flattening" them out.
  override def orElse(that: ToggleMap): ToggleMap = that

}
