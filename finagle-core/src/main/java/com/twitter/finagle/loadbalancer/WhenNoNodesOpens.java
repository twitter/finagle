package com.twitter.finagle.loadbalancer;

/**
 * Java API for {@link WhenNoNodesOpen}.
 */
public final class WhenNoNodesOpens {

  private WhenNoNodesOpens() {
    throw new IllegalStateException();
  }

  /** See WhenNoNodesOpen.PickOne. */
  public static final WhenNoNodesOpen PICK_ONE =
      WhenNoNodesOpen.PickOne$.MODULE$;

  /** See WhenNoNodesOpen.FailFast */
  public static final WhenNoNodesOpen FAIL_FAST =
      WhenNoNodesOpen.FailFast$.MODULE$;

}
