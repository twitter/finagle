package com.twitter.finagle.memcached.integration;

import java.nio.charset.Charset;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.memcached.java.Client;
import com.twitter.finagle.memcached.java.ClientBase;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.finagle.memcached.protocol.text.Memcached;

import junit.framework.TestCase;

public class TestClient extends TestCase {
  public static void main(String[] args) {
    System.out.println(new TestClient().run().wasSuccessful());
  }

  public void testGetAndSet() {
    Service<Command, Response> service =
      ClientBuilder.safeBuild(
        ClientBuilder
          .get()
          .hosts("localhost:11211")
          .codec(new Memcached(null))
          .hostConnectionLimit(1));

    Client client = ClientBase.newInstance(service);
    client.delete("foo").get();
    client.set("foo", "bar").get();
    System.out.println("hello?");
    client.get("foo").get().toString(Charset.defaultCharset());
  }
}
