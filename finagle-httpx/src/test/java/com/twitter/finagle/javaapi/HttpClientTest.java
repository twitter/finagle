package com.twitter.finagle.javaapi;

import java.util.Collection;
import java.util.List;

import com.twitter.finagle.Service;
import com.twitter.finagle.Client;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.httpx.Http;
import com.twitter.finagle.httpx.Response;
import com.twitter.finagle.httpx.Request;

import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

public class HttpClientTest {
  public static void main(String args[]) {
    Service<Request, Response> client =
      ClientBuilder.safeBuild(ClientBuilder.get()
        .codec(Http.get())
        .hosts("localhost:10000")
        .hostConnectionLimit(1));

    Request request = Request.apply("/");

    Future<Response> response = client.apply(request);
    response.addEventListener(
      new FutureEventListener<Response>() {
        public void onSuccess(Response response) {
          System.out.println("received response: " + response);
          System.exit(0);
        }

        public void onFailure(Throwable cause) {
          System.out.println("failed with cause: " + cause);
          System.exit(1);
        }
      });

    /* If the following compiles, variance annotations work (maybe!). */
    Promise<List<Integer>> promise = new Promise();
    promise.addEventListener(
      new FutureEventListener<Collection<Integer>>() {
        public void onSuccess(Collection<Integer> coll) {}
        public void onFailure(Throwable cause) {}
      });

    /* The following should *not* compile. Uncomment to test manually. */
    /*
    Promise<Collection<Integer>> badPromise = new Promise();
    badPromise.addEventListener(
      new FutureEventListener<List<Integer>>() {
        public void onSuccess(List<Integer> coll) {}
        public void onFailure(Throwable cause) {}
      });
    */

    // New APIs
    com.twitter.finagle.Http.newClient(":80");
    Client<Request, Response> newStyleClient =
      com.twitter.finagle.Http.client().withTls("foo.com");
  }
}
