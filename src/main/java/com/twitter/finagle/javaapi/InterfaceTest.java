package com.twitter.finagle.javaapi;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.handler.codec.http.*;

import com.twitter.finagle.client.*;
import com.twitter.util.*;

class InterfaceTest {
  public static void main(String args[]) {
    Client<HttpRequest, HttpResponse> client =
      Builder.apply()
        .hosts("localhost:10000,localhost:10001")
        .codec(Http$.MODULE$)
        .buildClient();

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
