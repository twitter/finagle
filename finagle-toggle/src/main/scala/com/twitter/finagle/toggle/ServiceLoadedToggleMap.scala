package com.twitter.finagle.toggle

import com.twitter.app.LoadService

/**
 * A `ServiceLoadedToggleMap` is a [[ToggleMap]] that uses util-app's
 * service loading mechanism (see `com.twitter.app.LoadService`) to be loaded.
 *
 * @see [[StandardToggleMap]]
 */
trait ServiceLoadedToggleMap extends ToggleMap {

  /**
   * The identifier for this library.
   *
   * Used so that multiple libraries can each have their own
   * backing [[ServiceLoadedToggleMap]].
   */
  def libraryName: String

}

object ServiceLoadedToggleMap {

  /**
   * Uses `com.twitter.app.LoadService` to find at most one [[ServiceLoadedToggleMap]]
   * with the given libraryName`.
   *
   * If no matches are found, a [[NullToggleMap]] is returned.
   *
   * '''Note:''' only 0 or 1 matches are allowed, anything more
   * will cause an `IllegalStateException` to be thrown. This is done
   * so that which [[ToggleMap]] used at runtime is deterministic.
   *
   * @param libraryName the name of the [[ServiceLoadedToggleMap.libraryName]] that
   *                   must match to be considered for use. The names should be
   *                   in fully-qualified form to avoid collisions, e.g.
   *                   "com.twitter.finagle".
   */
  def apply(libraryName: String): ToggleMap = {
    val toggleMaps =
      LoadService[ServiceLoadedToggleMap]().filter(_.libraryName == libraryName)

    if (toggleMaps.isEmpty)
      NullToggleMap
    else if (toggleMaps.size == 1)
      toggleMaps.head
    else {
      throw new IllegalStateException(
        s"Must have at most 1 `ServiceLoadedToggleMap` for libraryName=$libraryName, " +
          s"found ${toggleMaps.size}: ${toggleMaps.mkString(", ")}"
      )
    }
  }

}
