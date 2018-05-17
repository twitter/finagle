package com.twitter.finagle.loadbalancer;

import scala.collection.Seq;

import org.junit.Assert;
import org.junit.Test;

public class BalancerRegistryCompilationTest {

  @Test
  public void testBalancerRegistryGet() {
    BalancerRegistry registry = BalancerRegistry.get();
    Assert.assertNotNull(registry);
  }

  @Test
  public void testAllMetadata() {
    BalancerRegistry registry = BalancerRegistry.get();
    Seq<Metadata> mds = registry.allMetadata();
    Assert.assertNotNull(mds);
  }

}
