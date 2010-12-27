package com.twitter.finagle.javaapi;

import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.service.Service;
import com.twitter.finagle.builder.*;
import com.twitter.util.Future;
import com.twitter.util.*;

public class HttpClientTest {
  public static void main(String args[]) {
    Service<HttpRequest, HttpResponse> client =
      ClientBuilder
        .get()
        .hosts("localhost:10000")
        .codec(Codec4J.Http)
        .buildService();

    Future<HttpResponse> response =
      client.apply(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));

    response.addEventListener(
      new FutureEventListener<HttpResponse>() {
        public void onSuccess(HttpResponse response) {
          System.out.println("received response: " + response);
          System.exit(0);
        }

        public void onFailure(Throwable cause) {
          System.out.println("failed with cause: " + cause);
          System.exit(1);
        }
      });
  }
}
