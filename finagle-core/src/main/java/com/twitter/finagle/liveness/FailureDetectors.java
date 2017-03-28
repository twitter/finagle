package com.twitter.finagle.liveness;

/**
 * A Java adaptation of the {@link com.twitter.finagle.liveness.FailureDetector} companion object.
 */
public final class FailureDetectors {
  private FailureDetectors() { }

  /**
   * Default config type for {FailureDetector}
   *
   * @see FailureDetector$#globalFlagConfig
   */
  public static final FailureDetector.Config GLOBAL_FLAG_CONFIG =
    FailureDetector.GlobalFlagConfig$.MODULE$;

  /**
   * Null config type for {FailureDetector}
   *
   * @see FailureDetector$#nullConfig
   */
  public static final FailureDetector.Config NULL_CONFIG =
    FailureDetector.NullConfig$.MODULE$;
}
