package com.twitter.finagle.memcachedx.java;

import scala.Option;

import com.google.common.collect.ImmutableSet;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.memcachedx.CacheNode;
import com.twitter.finagle.memcachedx.CachePoolCluster;
import com.twitter.finagle.memcachedx.KetamaClientBuilder;
import com.twitter.finagle.memcachedx.protocol.Command;
import com.twitter.finagle.memcachedx.protocol.Response;
import com.twitter.finagle.memcachedx.protocol.text.Memcached;
import com.twitter.io.Buf;

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
    com.twitter.finagle.memcachedx.Client memcachedClient = KetamaClientBuilder.get()
        .cachePoolCluster(cluster)
        .clientBuilder(builder)
        .build();

    client = new ClientBase(memcachedClient);
    testClient(client);

  }

  public static void testClient(Client client) {
    client.delete("foo").get();
    client.set("foo", "bar").get();
    Option<String> res = Buf.Utf8$.MODULE$.unapply(client.get("foo").get());
    assert "bar".equals(res.get());
    ResultWithCAS casRes = client.gets("foo").get();
    assert client.cas("foo", "baz", casRes.casUnique).get();

    Option<String> res2 = Buf.Utf8$.MODULE$.unapply(client.get("foo").get());
    assert "baz".equals(res2.get());
    client.delete("foo").get();
    System.err.println("passed.");
    client.release();
  }
}
