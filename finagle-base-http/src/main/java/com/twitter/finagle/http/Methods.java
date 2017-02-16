package com.twitter.finagle.http;

/**
 * Java friendly versions of {@link com.twitter.finagle.http.Method}.
 *
 * @deprecated as of 2017-02-13. Please use the {@link com.twitter.finagle.http.Method}
 * instances directly.
 */
@Deprecated
public final class Methods {
  private Methods() { }

  public static final Method GET = Method.Get();
  public static final Method POST = Method.Post();
  public static final Method PUT = Method.Put();
  public static final Method HEAD = Method.Head();
  public static final Method PATCH = Method.Patch();
  public static final Method DELETE = Method.Delete();
  public static final Method TRACE = Method.Trace();
  public static final Method CONNECT = Method.Connect();
  public static final Method OPTIONS = Method.Options();

  /**
   * Construct a new {@link com.twitter.finagle.http.Method}.
   */
  public static Method newMethod(String name) {
    return Method.apply(name);
  }
}
