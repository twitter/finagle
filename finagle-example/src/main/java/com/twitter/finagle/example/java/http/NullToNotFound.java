package com.twitter.finagle.example.java.http;

import java.util.Objects;

import com.twitter.finagle.Service;
import com.twitter.finagle.SimpleFilter;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.finagle.http.Status;
import com.twitter.util.Function;
import com.twitter.util.Future;

/**
 * A simple Finagle filter that handles Responses with null and convert them to NotFound Responses.
 */
public final class NullToNotFound extends SimpleFilter<Request, Response> {

    @Override
    public Future<Response> apply(Request req, Service<Request, Response> service) {
        return service.apply(req).map(new Function<Response, Response>() {
            @Override
            public Response apply(Response resp) {
                if (Objects.equals(resp.contentString(), "null")) {
                    return Response.apply(Status.NotFound());
                }

                return resp;
            }
        });
    }
}
