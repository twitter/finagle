package com.twitter.finagle.ssl.client;

import java.util.Optional;

import scala.Option;

import org.junit.Test;

import com.twitter.util.javainterop.Scala;

// TODO: Uncomment when config types are made public.
// import com.twitter.finagle.ssl.ApplicationProtocolsConfig;
// import com.twitter.finagle.ssl.CipherSuitesConfig;
// import com.twitter.finagle.ssl.KeyCredentialsConfig;
// import com.twitter.finagle.ssl.ProtocolsConfig;
// import com.twitter.finagle.ssl.TrustCredentialsConfig;

public class SslClientConfigurationCompilationTest {

  @Test
  public void testConfiguration() {
    Option<String> hostname = Scala.asOption(Optional.empty());

    // TODO: Uncomment when config types are made public.
    // SslClientConfiguration config = new SslClientConfiguration(
    //     hostname,
    //     KeyCredentialsConfig.UNSPECIFIED,
    //     TrustCredentialsConfig.UNSPECIFIED,
    //     CipherSuitesConfig.UNSPECIFIED,
    //     ProtocolsConfig.UNSPECIFIED,
    //     ApplicationProtocolsConfig.UNSPECIFIED);
  }

}
