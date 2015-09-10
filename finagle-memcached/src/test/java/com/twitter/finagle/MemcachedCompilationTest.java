package com.twitter.finagle;

import com.twitter.finagle.memcached.Client;
import com.twitter.hashing.KeyHashers;

public class MemcachedCompilationTest {
  void createClient() {
    Client client = Memcached.client()
      .withKeyHasher(KeyHashers.FNV1_32())
      .withEjectFailedHost(true)
      .newRichClient("localhost:11211");
  }
}
