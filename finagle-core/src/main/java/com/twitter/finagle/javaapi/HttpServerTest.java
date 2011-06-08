package com.twitter.finagle.javaapi;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.*;
import com.twitter.finagle.builder.*;
import com.twitter.finagle.http.Http;
import com.twitter.util.Future;
import com.twitter.util.*;

public class HttpServerTest {
  private static void runServer() {
    Service<HttpRequest, HttpResponse> service =
      new Service<HttpRequest, HttpResponse>() {
        public Future<HttpResponse> apply(HttpRequest request) {
          HttpResponse httpResponse = new DefaultHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          // Respond right away.
          httpResponse.setContent(ChannelBuffers.wrappedBuffer("yo".getBytes()));

          Future<HttpResponse> future = Future.value(httpResponse);
          return future;
        }
      };

    ServerBuilder.safeBuild(
      service,
      ServerBuilder.get()
      .codec(Http.get())
      .bindTo(new InetSocketAddress("localhost", 10000))
      .name("HttpServer"));
  }

  public static void main(String args[]) {
    try {
      runServer();
    } catch (Throwable e) {
      System.err.println("Caught top level exception: " + e);
      e.printStackTrace();
      System.exit(-1);
    }
  }
}