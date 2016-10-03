package com.twitter.finagle.example.java.http;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.twitter.finagle.Http;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Service;
import com.twitter.finagle.http.HttpMuxer;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.Await;

public final class HttpServer {

    private static final InetSocketAddress ADDR = new InetSocketAddress(
        InetAddress.getLoopbackAddress(), 8888);

    private HttpServer() {

    }

    /**
     * Start the server.
     */
    public static void main(String[] args) throws Exception {
        LoggingFilter accessLog = new LoggingFilter();
        NullToNotFound nullFilter = new NullToNotFound();
        HandleErrors errorsFilter = new HandleErrors();
        Service<Request, Response> service = accessLog
            .andThen(nullFilter)
            .andThen(errorsFilter)
            .andThen(router());

        ListeningServer server = Http.server().serve(ADDR, service);

        Await.ready(server);
    }

    private static HttpMuxer router() {
        return new HttpMuxer()
                .withHandler("/cat", Handlers.echoHandler());
    }
}

