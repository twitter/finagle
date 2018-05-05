package com.twitter.finagle.ssl;

import javax.net.ssl.TrustManagerFactory;
import java.io.File;

/**
 * Java APIs for {@link TrustCredentials}.
 */
public final class TrustCredentialsConfig {

  private TrustCredentialsConfig() {
    throw new IllegalStateException();
  }

  /**
   * See {@link TrustCredentials.Unspecified}
   */
  public static final TrustCredentials UNSPECIFIED =
    TrustCredentials.Unspecified$.MODULE$;

  /**
   * See {@link TrustCredentials.Insecure}
   */
  public static final TrustCredentials INSECURE =
    TrustCredentials.Insecure$.MODULE$;

  /**
   * See {@link TrustCredentials.CertCollection}
   */
  public static TrustCredentials certCollection(File file) {
    return new TrustCredentials.CertCollection(file);
  }

  /**
   * See {@link TrustCredentials.FromTrustManagerFactory}
   */
  public static TrustCredentials fromTrustManagerFactory(TrustManagerFactory trustManagerFactory) {
    return new TrustCredentials.FromTrustManagerFactory(trustManagerFactory);
  }

}
