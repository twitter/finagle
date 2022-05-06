package com.twitter.finagle.ssl.server;

import org.junit.Test;

import com.twitter.finagle.ssl.ApplicationProtocolsConfig;
import com.twitter.finagle.ssl.CipherSuitesConfig;
import com.twitter.finagle.ssl.ClientAuthConfig;
import com.twitter.finagle.ssl.KeyCredentialsConfig;
import com.twitter.finagle.ssl.ProtocolsConfig;
import com.twitter.finagle.ssl.TrustCredentialsConfig;

public class SslServerConfigurationCompilationTest {

  @Test
  public void testConfiguration() {
    SslServerConfiguration config = new SslServerConfiguration(
      KeyCredentialsConfig.UNSPECIFIED,
      ClientAuthConfig.UNSPECIFIED,
      TrustCredentialsConfig.UNSPECIFIED,
      CipherSuitesConfig.UNSPECIFIED,
      ProtocolsConfig.UNSPECIFIED,
      ApplicationProtocolsConfig.UNSPECIFIED,
      false);
  }

}
