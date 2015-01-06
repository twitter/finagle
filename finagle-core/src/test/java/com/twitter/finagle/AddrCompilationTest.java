package com.twitter.finagle;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;

import com.google.common.collect.Lists;

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
    List<SocketAddress> list = Lists.<SocketAddress>newArrayList(
        new InetSocketAddress(0),
        new InetSocketAddress(0)
    );

    Addr a = Addrs.newBoundAddr(list.toArray(new SocketAddress[list.size()]));
    Addr b = Addrs.newBoundAddr(list);

    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
  }

  @Test
  public void testFailed() {
    Addr a = Addrs.newFailedAddr(new RuntimeException());
    Addr b = Addrs.newFailedAddr("because");

    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
  }
}
