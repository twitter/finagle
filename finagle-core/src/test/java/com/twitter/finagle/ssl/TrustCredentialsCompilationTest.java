package com.twitter.finagle.ssl;

import java.io.File;

import org.junit.Test;

public class TrustCredentialsCompilationTest {

  @Test
  public void testUnspecified() {
    TrustCredentials trustCredentials = TrustCredentialsConfig.UNSPECIFIED;
  }

  @Test
  public void testInsecure() {
    TrustCredentials trustCredentials = TrustCredentialsConfig.INSECURE;
  }

  @Test
  public void testCertCollection() {
    File file = null;
    TrustCredentials trustCredentials = TrustCredentialsConfig.certCollection(file);
  }

}
