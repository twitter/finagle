package com.twitter.finagle.memcached.java;

import scala.Option;

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
import com.twitter.io.Buf;
import com.twitter.util.Await;

/**
 * This is mainly for internal testing, not for external purpose
 */
public final class ClientTest {

  private ClientTest() { }

  public static void main(String[] args) throws Exception {
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

  public static void testClient(Client client) throws Exception {
    Await.result(client.delete("foo"));
    Await.result(client.set("foo", "bar"));
    Option<String> res = Buf.Utf8$.MODULE$.unapply(Await.result(client.get("foo")));
    assert "bar".equals(res.get());
    ResultWithCAS casRes = Await.result(client.gets("foo"));
    assert Await.result(client.cas("foo", "baz", casRes.casUnique));

    Option<String> res2 = Buf.Utf8$.MODULE$.unapply(Await.result(client.get("foo")));
    assert "baz".equals(res2.get());
    Await.result(client.delete("foo"));
    System.err.println("passed.");
    client.release();
  }
}
