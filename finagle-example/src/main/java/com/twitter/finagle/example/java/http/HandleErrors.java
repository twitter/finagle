package com.twitter.finagle.example.java.http;

import java.nio.charset.StandardCharsets;

import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.twitter.finagle.Service;
import com.twitter.finagle.SimpleFilter;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.Future;

/**
 * A simple Finagle filter that intercepts Exceptions and converts them to a more comprehensible HTTP Response.
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
                    resp.setContent(
                        ChannelBuffers.wrappedBuffer(
                            in.getMessage().getBytes(StandardCharsets.UTF_8)));

                    return resp;
                }
                resp.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
                resp.setContent(
                    ChannelBuffers.wrappedBuffer(in.getMessage().getBytes(StandardCharsets.UTF_8)));

                return resp;
            }
        });
    }
}
