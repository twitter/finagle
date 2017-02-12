package com.twitter.finagle;

import com.twitter.finagle.mux.Request;
import com.twitter.finagle.mux.Requests;
import com.twitter.finagle.mux.Response;
import com.twitter.finagle.mux.Responses;
import com.twitter.io.Bufs;
import com.twitter.util.Future;

// A compilation test for the Mux API.
public class UseMux {
  static {
    ServiceFactory<Request, Response> client = Mux.newClient(":8080");

    Mux.serve("localhost:*", new Service<Request, Response>() {
      public Future<Response> apply(Request req) {
        return Future.value(Responses.empty());
      }
    });

    Request request = Requests.empty();
    request = Requests.make(Path.empty(), Bufs.EMPTY);

    Response response = Responses.empty();
    response = Responses.make(Bufs.EMPTY);
  }
}
