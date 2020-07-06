package com.twitter.finagle.ssl;

/**
 * Java APIs for {@link OpportunisticTls}.
 */
public final class OpportunisticTlsConfig {

  private OpportunisticTlsConfig() {
    throw new IllegalStateException();
  }

  /**
   * See {@link OpportunisticTls.Off}
   */
  public static final OpportunisticTls.Level OFF =
      OpportunisticTls.Off$.MODULE$;

  /**
   * See {@link OpportunisticTls.Desired}
   */
  public static final OpportunisticTls.Level DESIRED =
      OpportunisticTls.Desired$.MODULE$;

  /**
   * See {@link OpportunisticTls.Required}
   */
  public static final OpportunisticTls.Level REQUIRED =
      OpportunisticTls.Required$.MODULE$;

}
