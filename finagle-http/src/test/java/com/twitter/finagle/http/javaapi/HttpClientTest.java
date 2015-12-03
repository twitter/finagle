package com.twitter.finagle.http.javaapi;

import java.util.Collection;
import java.util.List;

import scala.Tuple2;

import com.twitter.finagle.Client;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.http.Http;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.finagle.param.Label;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

public final class HttpClientTest {

  private HttpClientTest() { }

  /**
   * Runs the server, making sure the API is accessible in Java.
   */
  public static void main(String[] args) {
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
          throw new RuntimeException();
        }

        public void onFailure(Throwable cause) {
          System.out.println("failed with cause: " + cause);
          throw new RuntimeException();
        }
      });

    /* If the following compiles, variance annotations work (maybe!). */
    Promise<List<Integer>> promise = new Promise<List<Integer>>();
    promise.addEventListener(
      new FutureEventListener<Collection<Integer>>() {
        public void onSuccess(Collection<Integer> coll) { }
        public void onFailure(Throwable cause) { }
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

    // New API Compilation Test
    com.twitter.finagle.Http.newClient(":*");
    Client<Request, Response> newStyleClient =
      com.twitter.finagle.Http
          .client()
          .withTls("foo.com")
          .configured(Tuple2.apply(Label.apply("test"), Label.param()))
          .withDecompression(true);
  }
}
