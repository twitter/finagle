package com.twitter.finagle.javaapi;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.stub.*;
import com.twitter.finagle.builder.*;
import com.twitter.util.*;

public class ServerTest {
  public static void main(String args[]) {
    Stub<HttpRequest, HttpResponse> stub =
      new Stub<HttpRequest, HttpResponse>() {
        public Future<HttpResponse> call(HttpRequest request) {
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
      .stub(stub)
      .bindTo(new InetSocketAddress("localhost", 10000))
      .build();
  }
}