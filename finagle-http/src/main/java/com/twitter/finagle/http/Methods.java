package com.twitter.finagle.http;

/**
 * Java friendly versions of {@link com.twitter.finagle.http.Method}.
 */
public final class Methods {
  private Methods() { }

  public static final Method GET = Method.Get$.MODULE$;
  public static final Method POST = Method.Post$.MODULE$;
  public static final Method PUT = Method.Put$.MODULE$;
  public static final Method HEAD = Method.Head$.MODULE$;
  public static final Method PATCH = Method.Patch$.MODULE$;
  public static final Method DELETE = Method.Delete$.MODULE$;
  public static final Method TRACE = Method.Trace$.MODULE$;
  public static final Method CONNECT = Method.Connect$.MODULE$;
  public static final Method OPTIONS = Method.Options$.MODULE$;

  /**
   * Construct a new {@link com.twitter.finagle.http.Method}.
   */
  public static Method newMethod(String name) {
    return Method.apply(name);
  }
}
