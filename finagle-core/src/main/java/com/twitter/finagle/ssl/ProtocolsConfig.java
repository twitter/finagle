package com.twitter.finagle.ssl;

import java.util.List;

import com.twitter.util.javainterop.Scala;

/**
 * Java APIs for {@link Protocols}.
 */
public final class ProtocolsConfig {

  private ProtocolsConfig() {
    throw new IllegalStateException();
  }

  /**
   * See {@link Protocols.Unspecified}
   */
  public static final Protocols UNSPECIFIED =
    Protocols.Unspecified$.MODULE$;

  /**
   * See {@link Protocols.Enabled}
   */
  public static Protocols enabled(List<String> protocols) {
    return new Protocols.Enabled(Scala.asImmutableSeq(protocols));
  }

}
