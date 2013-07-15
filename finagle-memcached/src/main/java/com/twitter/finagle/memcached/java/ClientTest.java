package com.twitter.finagle.memcached.java;

import java.nio.charset.Charset;

import com.google.common.collect.ImmutableSet;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.memcached.CacheNode;
import com.twitter.finagle.memcached.CachePoolCluster;
import com.twitter.finagle.memcached.KetamaClientBuilder;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.finagle.memcached.protocol.text.Memcached;

/**
 * This is mainly for internal testing, not for external purpose
 */
public class ClientTest {
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
    assert(client.get("foo").get().toString(Charset.defaultCharset()) == "bar");
    ResultWithCAS res = client.gets("foo").get();
    assert(client.cas("foo", "baz", res.casUnique).get());
    assert(client.get("foo").get().toString(Charset.defaultCharset()) == "baz");
    client.delete("foo").get();
    System.err.println("passed.");
    client.release();
  }
}
