package com.twitter.finagle;

import com.twitter.finagle.mux.Ask;
import com.twitter.finagle.mux.Asks;
import com.twitter.finagle.mux.Response;
import com.twitter.finagle.mux.Responses;
import com.twitter.io.Buf$;
import com.twitter.util.Future;

// A compilation test for the Mux API.
public class UseMux {
  static {
    ServiceFactory<Ask, Response> client = Mux.newClient(":8080");

    Mux.serve("localhost:*", new Service<Ask, Response>() {
      public Future<Response> apply(Ask req) {
        return Future.value(Responses.empty());
      }
    });

    // TODO Buf$ should be replaced by Bufs.

    Ask request = Asks.empty();
    request = Asks.make(Path.empty(), Buf$.MODULE$.Empty());

    Response response = Responses.empty();
    response = Responses.make(Buf$.MODULE$.Empty());
  }
}
