package com.twitter.finagle.javaapi;

import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.*;
import com.twitter.finagle.builder.*;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.*;

public class HttpClientTest {
  public static void main(String args[]) {
    Service<HttpRequest, HttpResponse> client =
      ClientBuilder.safeBuild(ClientBuilder.get()
        .codec(Codec4J.Http)
        .hosts("localhost:10000")
        .hostConnectionLimit(1));

    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");

    Future<HttpResponse> response = client.apply(request);
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
