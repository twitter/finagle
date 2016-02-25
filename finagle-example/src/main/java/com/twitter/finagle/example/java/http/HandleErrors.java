package com.twitter.finagle.example.java.http;

import com.twitter.finagle.Service;
import com.twitter.finagle.SimpleFilter;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.Future;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * A simple Finagle that intercepts Exceptions and converts them to a more comprehensible HTTP Response.
 */
public final class HandleErrors extends SimpleFilter<Request, Response> {

    @Override
    public Future<Response> apply(Request req, Service<Request, Response> service) {
        return service.apply(req).handle(new ExceptionalFunction<Throwable, Response>() {
            @Override
            public Response applyE(Throwable in) {
                Response resp = Response.apply();
                if (in instanceof NumberFormatException) {
                    resp.setStatus(HttpResponseStatus.BAD_REQUEST);
                    resp.setContent(ChannelBuffers.wrappedBuffer(in.getMessage().getBytes()));

                    return resp;
                }
                in.printStackTrace();
                resp.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                resp.setContent(ChannelBuffers.wrappedBuffer(in.getMessage().getBytes()));

                return resp;
            }
        });
    }
}
