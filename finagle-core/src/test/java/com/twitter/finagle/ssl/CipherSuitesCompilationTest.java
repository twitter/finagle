package com.twitter.finagle.ssl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class CipherSuitesCompilationTest {

  @Test
  public void testUnspecified() {
    CipherSuites cipherSuites = CipherSuitesConfig.UNSPECIFIED;
  }

  @Test
  public void testEnabled() {
    List<String> items = new ArrayList<String>();
    items.add("TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384");
    items.add("TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");
    CipherSuites cipherSuites = CipherSuitesConfig.enabled(items);
  }

  @Test
  public void testFromString() {
    CipherSuites cipherSuites = CipherSuitesConfig.fromString(
      "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384:TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384");
  }

}
