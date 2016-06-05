package com.twitter.finagle.http;

/**
 * Java friendly versions of {@link com.twitter.finagle.http.Version}.
 */
public final class Versions {
  private Versions() { }

  public static final Version HTTP_1_1 = Version.Http11$.MODULE$;
  public static final Version HTTP_1_0 = Version.Http10$.MODULE$;
}
