package com.twitter.finagle.httpx;

/**
 * Java friendly versions of {@link com.twitter.finagle.httpx.Version}.
 */
public final class Versions {
  private Versions() { }

  public static final Version HTTP_1_1 = Version.Http11$.MODULE$;
  public static final Version HTTP_1_0 = Version.Http10$.MODULE$;
}
