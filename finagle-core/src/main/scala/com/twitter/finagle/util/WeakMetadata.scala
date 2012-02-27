package com.twitter.finagle.util

import com.twitter.util.MapMaker

/**
 * A convenience constructor for keeping weakly referenced metadata.
 */
object WeakMetadata {
  def apply[V](default: => V) =
    MapMaker[Any, V] { config =>
      config.compute { key =>
        default
      }
      config.weakKeys
    }
}
