package com.twitter.finagle.ssl;

import java.io.File;
import javax.net.ssl.KeyManagerFactory;

/**
 * Java APIs for {@link KeyCredentials}.
 */
public final class KeyCredentialsConfig {

  private KeyCredentialsConfig() {
    throw new IllegalStateException();
  }

  /**
   * See {@link KeyCredentials.Unspecified}
   */
  public static final KeyCredentials UNSPECIFIED =
    KeyCredentials.Unspecified$.MODULE$;

  /**
   * See {@link KeyCredentials.CertAndKey}
   */
  public static KeyCredentials certAndKey(
      File certificateFile, File keyFile) {
    return new KeyCredentials.CertAndKey(certificateFile, keyFile);
  }

  /**
   * See {@link KeyCredentials.CertsAndKey}
   */
  public static KeyCredentials certsAndKey(
      File certificatesFile, File keyFile) {
    return new KeyCredentials.CertsAndKey(certificatesFile, keyFile);
  }

  /**
   * See {@link KeyCredentials.CertKeyAndChain}
   */
  public static KeyCredentials certKeyAndChain(
      File certificateFile, File keyFile, File caCertificateFile) {
    return new KeyCredentials.CertKeyAndChain(certificateFile, keyFile, caCertificateFile);
  }

  /**
   * See {@link KeyCredentials.KeyManagerFactory}
   */
  public static KeyCredentials keyManagerFactory(KeyManagerFactory keyManagerFactory) {
    return new KeyCredentials.KeyManagerFactory(keyManagerFactory);
  }
}
