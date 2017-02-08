package com.twitter.finagle.ssl;

import java.util.List;

import com.twitter.util.javainterop.Scala;

/**
 * Java APIs for {@link CipherSuites}.
 */
public final class CipherSuitesConfig {

  private CipherSuitesConfig() {
    throw new IllegalStateException();
  }

  /**
   * See {@link CipherSuites.Unspecified}
   */
  public static final CipherSuites UNSPECIFIED =
    CipherSuites.Unspecified$.MODULE$;

  /**
   * See {@link CipherSuites.Enabled}
   */
  public static CipherSuites enabled(List<String> ciphers) {
    return new CipherSuites.Enabled(Scala.asImmutableSeq(ciphers));
  }

  /**
   * See {@link CipherSuites.fromString}
   */
  public static CipherSuites fromString(String ciphers) {
    return CipherSuites$.MODULE$.fromString(ciphers);
  }

}
