package com.twitter.finagle.httpx.javaapi;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.jboss.netty.buffer.ChannelBuffers;

import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.httpx.Http;
import com.twitter.finagle.httpx.Request;
import com.twitter.finagle.httpx.Response;
import com.twitter.util.Future;

public final class HttpServerTest {

  private HttpServerTest() { }

  private static void runServer() {
    Service<Request, Response> service =
      new Service<Request, Response>() {
        public Future<Response> apply(Request request) {
          Response response = Response.apply();
          // Respond right away.
          response.setContent(ChannelBuffers.wrappedBuffer("yo".getBytes()));

          Future<Response> future = Future.value(response);
          return future;
        }
      };

    ServerBuilder.safeBuild(
      service,
        ServerBuilder.get()
            .codec(Http.get())
            .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
      .name("HttpServer"));
  }

  public static void main(String[] args) {
    runServer();
  }
}
