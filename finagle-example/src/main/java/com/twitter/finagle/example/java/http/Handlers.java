package com.twitter.finagle.example.java.http;

import static java.lang.Integer.parseInt;

import com.twitter.finagle.Service;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.finagle.http.Status;
import com.twitter.io.Bufs;
import com.twitter.util.Future;

import static com.twitter.finagle.example.java.http.JsonUtils.toBytes;

public final class Handlers {

    private Handlers() {

    }

    static Service<Request, Response> echoHandler() {
        return new Service<Request, Response>() {
            public Future<Response> apply(Request request) {
                Cat cat = CatService.find(getId(request));
                Response response = Response.apply(request.version(), Status.Ok());
                response.content(Bufs.ownedBuf(toBytes(cat)));

                return Future.value(response);
            }

            private int getId(Request request) {
                String id = request.getParam("id");

                try {
                    return parseInt(id);
                } catch (NumberFormatException e) {
                    // in general, its not a good practice to throw exceptions
                    // and instead recommend using `Future.exception()`, however
                    // this is a demonstration of the `HandleErrors` filter.
                    throw new NumberFormatException("Invalid id: " + id);
                }
            }
        };
    }
}

