package com.twitter.finagle.ssl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class ApplicationProtocolsCompilationTest {

  @Test
  public void testUnspecified() {
    ApplicationProtocols appProtocols = ApplicationProtocolsConfig.UNSPECIFIED;
  }

  @Test
  public void testSupported() {
    List<String> items = new ArrayList<String>();
    items.add("h2");
    items.add("spdy/3.1");
    ApplicationProtocols appProtocols = ApplicationProtocolsConfig.supported(items);
  }

  @Test
  public void testFromString() {
    ApplicationProtocols appProtocols = ApplicationProtocolsConfig.fromString("h2,spdy/3.1");
  }

}
