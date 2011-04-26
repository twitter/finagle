package com.twitter.finagle.memcached.java;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.memcached.protocol.Command;
import com.twitter.finagle.memcached.protocol.Response;
import com.twitter.finagle.memcached.protocol.text.Memcached;

import java.nio.charset.Charset;

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
    client.delete("foo").get();
    client.set("foo", "bar").get();
    assert(client.get("foo").get().toString(Charset.defaultCharset()) == "bar");
  }
}
