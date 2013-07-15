package com.twitter.finagle.memcached.integration;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import scala.Option;

import org.jboss.netty.buffer.ChannelBuffer;
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
import com.twitter.util.Await;

import static org.junit.Assert.assertEquals;

public class TestClient {
  private Option<InetSocketAddress> address;

  @Before
  public void setUp() {
    address = ExternalMemcached.start();
    Assume.assumeTrue(address.isDefined());
  }

  @Test
  public void testGetAndSet() throws Exception {
    Service<Command, Response> service =
      ClientBuilder.safeBuild(
        ClientBuilder
          .get()
          .hosts(address.get())
          .codec(new Memcached())
          .hostConnectionLimit(1));

    Client client = ClientBase.newInstance(service);
    Await.ready(client.set("foo", "bar"));
    assertEquals("bar",
        Await.<ChannelBuffer>result(client.get("foo")).toString(Charset.defaultCharset()));
  }
}
