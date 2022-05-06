package com.twitter.finagle.ssl.client;

import java.util.Optional;

import scala.Option;

import org.junit.Test;

import com.twitter.finagle.ssl.ApplicationProtocolsConfig;
import com.twitter.finagle.ssl.CipherSuitesConfig;
import com.twitter.finagle.ssl.KeyCredentialsConfig;
import com.twitter.finagle.ssl.ProtocolsConfig;
import com.twitter.finagle.ssl.TrustCredentialsConfig;
import com.twitter.util.javainterop.Scala;

public class SslClientConfigurationCompilationTest {

  @Test
  public void testConfiguration() {
    Option<String> hostname = Scala.asOption(Optional.empty());

    SslClientConfiguration config = new SslClientConfiguration(
      hostname,
      hostname,
      KeyCredentialsConfig.UNSPECIFIED,
      TrustCredentialsConfig.UNSPECIFIED,
      CipherSuitesConfig.UNSPECIFIED,
      ProtocolsConfig.UNSPECIFIED,
      ApplicationProtocolsConfig.UNSPECIFIED,
      false);
  }

}
