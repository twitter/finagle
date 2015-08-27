package com.twitter.finagle.memcached.integration;

import scala.Option;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.memcached.java.Client;
import com.twitter.finagle.memcached.java.ClientBase;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.finagle.memcached.protocol.text.Memcached;
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
    Service<Command, Response> service =
      ClientBuilder.safeBuild(
        ClientBuilder
          .get()
          .hosts(server.get().address())
          .codec(new Memcached())
          .hostConnectionLimit(1));

    Client client = ClientBase.newInstance(service);
    Await.ready(client.set("foo", "bar"));

    Option<String> res = Buf.Utf8$.MODULE$.unapply(Await.result(client.get("foo")));
    assertEquals("bar", res.get());
  }
}
