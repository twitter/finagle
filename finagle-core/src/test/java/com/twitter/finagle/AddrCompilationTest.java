package com.twitter.finagle;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Test;

public class AddrCompilationTest {

  @Test
  public void testNegAndPending() {
    Assert.assertNotNull(Addrs.negAddr());
    Assert.assertNotNull(Addrs.pendingAddr());
  }

  @Test
  public void testBound() {
    List<Address> list = Lists.<Address>newArrayList(
        Addresses.newInetAddress(new InetSocketAddress(0)),
        Addresses.newInetAddress(new InetSocketAddress(0))
    );

    Map<String, Object> meta = Maps.newHashMap();
    meta.put("foo", "bar");

    Addr a = Addrs.newBoundAddr(list.toArray(new Address[list.size()]));
    Addr b = Addrs.newBoundAddr(list);
    Addr c = Addrs.newBoundAddr(list, meta);

    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
    Assert.assertNotNull(c);
  }

  @Test
  public void testFailed() {
    Addr a = Addrs.newFailedAddr(new RuntimeException());
    Addr b = Addrs.newFailedAddr("because");

    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
  }
}
