package com.twitter.finagle;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class AddressCompilationTest {

  @Test
  public void testInet() {
    InetSocketAddress ia = new InetSocketAddress(0);

    Address a = Addresses.newInetAddress(ia);
    Assert.assertNotNull(a);

    Map<String, Object> meta = new HashMap<>();
    meta.put("foo", "bar");
    Address b = Addresses.newInetAddress(ia, meta);
    Assert.assertNotNull(b);
  }

  @Test
  public void testFailed() {
    Address a = Addresses.newFailedAddress(new Exception("boo"));
    Assert.assertNotNull(a);
  }
}
