package com.twitter.finagle.javaapi;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.service.*;
import com.twitter.finagle.builder.*;
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

          Promise<HttpResponse> future = new Promise<HttpResponse>();
          future.update(new Return<HttpResponse>(httpResponse));
          return future;
        }
      };

    ServerBuilder
      .get()
      .codec(Codec4J.http())
      .service(service)
      .bindTo(new InetSocketAddress("localhost", 10000))
      .build();
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