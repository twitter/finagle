package com.twitter.finagle;

import junit.framework.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

public class AddrCompilationTest {

  @Test
  public void testNegAndPending() {
    Addr a = Addrs.negAddr();
    Addr b = Addrs.pendingAddr();

    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
  }

  @Test
  public void testBound() {
    List<SocketAddress> list = new ArrayList<SocketAddress>() {{
      add(new InetSocketAddress(0));
      add(new InetSocketAddress(0));
    }};

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
