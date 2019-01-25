package com.twitter.finagle.ssl;

import java.io.File;

import org.junit.Test;

public class KeyCredentialsCompilationTest {

  @Test
  public void testUnspecified() {
    KeyCredentials keyCredentials = KeyCredentialsConfig.UNSPECIFIED;
  }

  @Test
  public void testCertAndKey() {
    File file = null;
    KeyCredentials keyCredentials = KeyCredentialsConfig.certAndKey(file, file);
  }

  @Test
  public void testCertsAndKey() {
    File file = null;
    KeyCredentials keyCredentials = KeyCredentialsConfig.certsAndKey(file, file);
  }

  @Test
  public void testCertKeyAndChain() {
    File file = null;
    KeyCredentials keyCredentials = KeyCredentialsConfig.certKeyAndChain(file, file, file);
  }

}
