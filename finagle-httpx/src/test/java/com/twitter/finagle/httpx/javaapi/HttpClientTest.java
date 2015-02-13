package com.twitter.finagle.httpx.javaapi;

import java.util.Collection;
import java.util.List;

import com.twitter.finagle.Client;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.httpx.Http;
import com.twitter.finagle.httpx.Request;
import com.twitter.finagle.httpx.Response;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.Promise;

public final class HttpClientTest {

  private HttpClientTest() { }

  /**
   * Rns the server with the given {@code args}.
   *
   * @param args the arguments array
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

    // New APIs
    com.twitter.finagle.Httpx.newClient(":80");
    Client<Request, Response> newStyleClient =
      com.twitter.finagle.Httpx.client().withTls("foo.com");
  }
}
