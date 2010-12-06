package com.twitter.finagle.javaapi;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.service.*;
import com.twitter.finagle.builder.*;
import com.twitter.util.*;

public class HttpServerTest {
  private static byte[] buf = new byte[10<<20];
  private static ChannelBuffer cb = ChannelBuffers.wrappedBuffer(buf);

  static {
    Arrays.fill(buf, (byte)'.');
  }

  private static void runServer() {
    Service<HttpRequest, HttpResponse> service =
      new Service<HttpRequest, HttpResponse>() {
        public Future<HttpResponse> apply(HttpRequest request) {
          HttpResponse httpResponse = new DefaultHttpResponse(
            HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

          int nBytes = 1024 * 2;
          httpResponse.setContent(new SlicedChannelBuffer(cb, 0, nBytes));
          httpResponse.setHeader("Content-Length", nBytes);
          httpResponse.setHeader("Connection", "close");

          Promise<HttpResponse> future = new Promise<HttpResponse>();
          future.update(new Return<HttpResponse>(httpResponse));
          return future;
        }
      };

    InetSocketAddress addr = new InetSocketAddress("localhost", 10000);
    System.out.println("Server listening on " + addr);
    ServerBuilder
      .get()
      .codec(Codec4J.http())
      .service(service)
      .bindTo(addr)
      .tls("keystore.jks", "password")
      .compressionLevel(6)
      .build();
  }

  public static void main(String args[]) {
    try {
      new HttpServerTest().runServer();
    } catch (Throwable e) {
      System.err.println("Caught top level exception: " + e);
      e.printStackTrace();
      System.exit(-1);
    }
  }
}