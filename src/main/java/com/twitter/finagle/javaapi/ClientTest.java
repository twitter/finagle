package com.twitter.finagle.javaapi;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.stub.Stub;
import com.twitter.finagle.builder.*;
import com.twitter.util.*;

public class ClientTest {
  public static void main(String args[]) {
    Stub<HttpRequest, HttpResponse> client =
      ClientBuilder.get()
        .hosts("localhost:10000")
        .codec(Codec4J.http())
        .buildStub();

    Future<HttpResponse> response =
      client.call(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));

    response.addEventListener(
      new FutureEventListener<HttpResponse>() {
        public void onSuccess(HttpResponse response) {
          System.out.println("received response: " + response);
        }

        public void onFailure(Throwable cause) {
          System.out.println("failed with cause: " + cause);
        }
      });
  }
}
