package com.twitter.finagle.memcached.java;

import java.nio.charset.Charset;

import com.google.common.collect.ImmutableSet;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.cacheresolver.CacheNode;
import com.twitter.finagle.cacheresolver.CachePoolCluster;
import com.twitter.finagle.cacheresolver.java.CachePoolClusterUtil;
import com.twitter.finagle.memcached.KetamaClientBuilder;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.finagle.memcached.protocol.text.Memcached;

/**
 * This is mainly for internal testing, not for external purpose
 */
public final class ClientTest {

  private ClientTest() { }

  public static void main(String[] args) {
    Service<Command, Response> service =
      ClientBuilder.safeBuild(
        ClientBuilder
          .get()
          .hosts("localhost:11211")
          .hostConnectionLimit(1)
          .codec(new Memcached()));

    Client client = Client.newInstance(service);
    testClient(client);

    // cache client with cluster
    CachePoolCluster cluster = CachePoolClusterUtil.newStaticCluster(
        ImmutableSet.of(new CacheNode("localhost", 11211, 1)));

    ClientBuilder builder = ClientBuilder.get().codec(new Memcached(null));
    com.twitter.finagle.memcached.Client memcachedClient = KetamaClientBuilder.get()
        .cachePoolCluster(cluster)
        .clientBuilder(builder)
        .build();

    client = new ClientBase(memcachedClient);
    testClient(client);

  }

  public static void testClient(Client client) {
    client.delete("foo").get();
    client.set("foo", "bar").get();
    assert "bar".equals(client.get("foo").get().toString(Charset.defaultCharset()));
    ResultWithCAS res = client.gets("foo").get();
    assert client.cas("foo", "baz", res.casUnique).get();
    assert "baz".equals(client.get("foo").get().toString(Charset.defaultCharset()));
    client.delete("foo").get();
    System.err.println("passed.");
    client.release();
  }
}
