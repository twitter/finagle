package com.twitter.finagle.ssl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class ProtocolsCompilationTest {

  @Test
  public void testUnspecified() {
    Protocols protocols = ProtocolsConfig.UNSPECIFIED;
  }

  @Test
  public void testEnabled() {
    List<String> items = new ArrayList<String>();
    items.add("TLSv1.2");
    items.add("TLSv1.1");
    Protocols protocols = ProtocolsConfig.enabled(items);
  }

}
