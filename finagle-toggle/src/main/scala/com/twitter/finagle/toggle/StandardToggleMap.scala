package com.twitter.finagle.toggle

/**
 * A [[ToggleMap]] that is the composition of multiple underlying
 * [[ToggleMap]] implementations in a specific ordering designed
 * to balance control between the operators/service-owners and the library
 * owners.
 *
 * The ordering is as such:
 *  i. The mutable, in-process [[ToggleMap]], provided via [[ToggleMap.mutable]].
 *  i. The `GlobalFlag`-backed [[ToggleMap]], provided via [[ToggleMap.flags]].
 *  i. (incomplete) The service-owner controlled file-based [[ToggleMap]].
 *  i. The dynamically loaded [[ToggleMap]], provided via [[ServiceLoadedToggleMap.apply]].
 *  i. (incomplete) The library-owner controlled file-based [[ToggleMap]].
 *
 * The expectation is that 1, 2, and 3 give service-owners/operators the tools
 * to operate and test [[Toggle toggles]] while library owners would have control
 * over 4 and 5.
 * Flags and in-memory settings allow for rapid testing and overrides
 * while file-based configs are for static configuration owners have settled on.
 */
object StandardToggleMap {

  /**
   * @param libraryName if multiple matching service loaded implementations are
   *                    found, this will fail with an `java.lang.IllegalStateException`.
   *                    The names should be in fully-qualified form to avoid
   *                    collisions, e.g. "com.twitter.finagle".
   */
  def apply(libraryName: String): ToggleMap =
    apply(libraryName, ToggleMap.mutable)

  // exposed for testing
  private[toggle] def apply(
    libraryName: String,
    mutable: ToggleMap
  ): ToggleMap = {
    ToggleMap.of(
      mutable,

      ToggleMap.flags,

      // todo: service owners config file (update scaladoc above)
      // `com.twitter.finagle.toggle.App-$libname.properties`
      NullToggleMap,

      ServiceLoadedToggleMap(libraryName),

      // todo: Default library configuration files, (update scaladoc above)
      // `com.twitter.finagle.toggle.Lib-$libname.properties`.
      NullToggleMap
    )
  }

}
