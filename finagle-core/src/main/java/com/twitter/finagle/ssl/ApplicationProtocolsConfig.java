package com.twitter.finagle.ssl;

import java.util.List;

import com.twitter.util.javainterop.Scala;

/**
 * Java APIs for {@link ApplicationProtocols}.
 */
public final class ApplicationProtocolsConfig {

  private ApplicationProtocolsConfig() {
    throw new IllegalStateException();
  }

  /**
   * See {@link ApplicationProtocols.Unspecified}
   */
  public static final ApplicationProtocols UNSPECIFIED =
    ApplicationProtocols.Unspecified$.MODULE$;

  /**
   * See {@link ApplicationProtocols.Supported}
   */
  public static ApplicationProtocols supported(List<String> appProtocols) {
    return new ApplicationProtocols.Supported(Scala.asImmutableSeq(appProtocols));
  }

  /**
   * See {@link ApplicationProtocols.fromString}
   */
  public static ApplicationProtocols fromString(String appProtocols) {
    return ApplicationProtocols$.MODULE$.fromString(appProtocols);
  }

}
