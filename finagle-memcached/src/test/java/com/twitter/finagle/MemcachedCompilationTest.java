package com.twitter.finagle;

import org.junit.Test;

import com.twitter.finagle.memcached.Client;
import com.twitter.finagle.param.Label;
import com.twitter.hashing.KeyHashers;

public class MemcachedCompilationTest {

  /**
   * Tests Java usage of the Memcached client. The client API should be as accessible in Java as it
   * is in Scala.
   */
  @Test
  public void testClientCompilation() {
    final Client client = Memcached.client()
        .withKeyHasher(KeyHashers.FNV1_32())
        .configured(Label.apply("test").mk())
        .withEjectFailedHost(true)
        .newRichClient("localhost:11211");
  }
}
