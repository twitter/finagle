package com.twitter.finagle.memcached.java;

import scala.Option;

import com.twitter.finagle.Memcached;
import com.twitter.finagle.Service;
import com.twitter.finagle.memcached.loadbalancer.ConcurrentLoadBalancerFactory;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.io.Buf;
import com.twitter.util.Await;

/**
 * This is mainly for internal testing, not for external purpose
 */
public final class ClientTest {

  private ClientTest() { }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    Service<Command, Response> service = Memcached.client()
          .configured(new ConcurrentLoadBalancerFactory.Param(1).mk())
          .newService("localhost:11211");

    Client client = Client.newInstance(service);
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
