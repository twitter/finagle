package com.twitter.finagle;

import java.net.InetSocketAddress;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.util.Future;

public class NameCompilationTest {

  @Test
  public void testPath() {
    Path path = Path.read("/foo/bar");
    Name.Path name = new Name.Path(path);
    Assert.assertEquals(path, name.path());
  }

  @Test
  public void testBound() {
    Name.Bound boundNone = Names.bound();

    Address addr = Addresses.newInetAddress(new InetSocketAddress(0));
    Name.Bound boundOne = Names.bound(addr);
    Name.Bound boundMultiple = Names.bound(addr, addr);
  }

  @Test
  public void testBoundService() {
    Name.Bound bound = Names.bound(Service.constant(Future.value("Hello")));
  }

  @Test
  public void testEmpty() {
    Name.Bound bound = Names.empty();
  }

  @Test
  public void testApplyPath() {
    Name name = Names.fromPath(Path.read("/foo/bar"));
  }

  @Test
  public void testApplyString() {
    Name name = Names.fromPath("/foo/bar");
  }

}
