package com.twitter.finagle.memcached.integration;

import java.util.ArrayList;

import scala.Option;
import scala.collection.JavaConversions;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.twitter.finagle.Address;
import com.twitter.finagle.Address$;
import com.twitter.finagle.Memcached;
import com.twitter.finagle.Name$;
import com.twitter.finagle.Service;
import com.twitter.finagle.memcached.JavaClient;
import com.twitter.finagle.memcached.JavaClientBase;
import com.twitter.finagle.memcached.integration.external.TestMemcachedServer;
import com.twitter.finagle.memcached.integration.external.TestMemcachedServer$;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.io.Buf;
import com.twitter.util.Await;

import static org.junit.Assert.assertEquals;

public class TestClient {
  private Option<TestMemcachedServer> server;

  @Before
  public void setUp() {
    server = TestMemcachedServer$.MODULE$.start();
    Assume.assumeTrue(server.isDefined());
  }

  /**
   * Tests Get/Set commands.
   */
  @Test
  public void testGetAndSet() throws Exception {
    Address addr = Address$.MODULE$.apply(server.get().address());
    ArrayList<Address> addrs = new ArrayList<Address>();
    addrs.add(addr);

    Service<Command, Response> service = Memcached.client()
          .connectionsPerEndpoint(1)
          .newService(Name$.MODULE$.bound(JavaConversions.asScalaBuffer(addrs)), "memcached");

    JavaClient client = JavaClientBase.newInstance(service);
    Await.ready(client.set("foo", "bar"));

    Option<String> res = Buf.Utf8$.MODULE$.unapply(Await.result(client.get("foo")));
    assertEquals("bar", res.get());
  }
}
