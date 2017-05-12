package com.twitter.finagle.http.javaapi;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.twitter.finagle.Http;
import com.twitter.finagle.Server;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.client.StackClient$;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.finagle.param.Label;
import com.twitter.util.Future;

public final class HttpServerTest {

  private HttpServerTest() { }

  private static void runServer() {
    Service<Request, Response> service =
      new Service<Request, Response>() {
        public Future<Response> apply(Request request) {
          Response response = Response.apply();
          // Respond right away.
          response.setContentString("yo");

          Future<Response> future = Future.value(response);
          return future;
        }
      };

    ServerBuilder.safeBuild(
      service,
        ServerBuilder.get()
            .stack(Http.server())
            .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))
      .name("HttpServer"));
  }

  /**
   * Runs the server, making sure the API is accessible in Java.
   */
  public static void main(String[] args) {
    runServer();

    // New API Compilation Test
    com.twitter.finagle.Http.newService(":*");
    Server<Request, Response> newStyleServer =
        com.twitter.finagle.Http
            .server()
            .withCompressionLevel(2)
            .configured(new Label("test").mk())
            .withDecompression(true)
            .configuredParams(StackClient$.MODULE$.defaultParams());
  }
}
