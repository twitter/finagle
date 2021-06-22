package com.twitter.finagle.http.javaapi;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.twitter.finagle.Http;
import com.twitter.finagle.Server;
import com.twitter.finagle.Service;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.finagle.param.Label;
import com.twitter.util.Future;
import com.twitter.util.StorageUnit;

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

    Http.server()
        .withLabel("HttpServer")
        .serve(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), service);
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
            .withAdmissionControl().deadlines()
            .withAdmissionControl().noDeadlines()
            .withAdmissionControl().darkModeDeadlines()
            .withCompressionLevel(2)
            .withStreaming(StorageUnit.fromMegabytes(1))
            .configured(new Label("test").mk())
            .withDecompression(true)
            .withHttp2();
  }
}
