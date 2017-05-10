package com.twitter.finagle.http;

/**
 * Java friendly versions of {@link com.twitter.finagle.http.Version}.
 *
 * @deprecated as of 2017-02-13. Please use the {@link com.twitter.finagle.http.Version}
 * instances directly.
 */
@Deprecated
public final class Versions {
  private Versions() { }

  public static final Version HTTP_1_1 = Version.Http11();
  public static final Version HTTP_1_0 = Version.Http10();
}
