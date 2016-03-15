package com.twitter.finagle.example.java.http;

import com.twitter.finagle.Http;
import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Service;
import com.twitter.finagle.http.HttpMuxer;
import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.util.Await;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class HttpServer {

    private final static InetSocketAddress addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 8888);

    public static void main(String[] args) throws Exception {
        LoggingFilter accessLog = new LoggingFilter();
        NullToNotFound nullFilter = new NullToNotFound();
        HandleErrors errorsFilter = new HandleErrors();
        Service<Request, Response> service = accessLog.andThen(nullFilter).andThen(errorsFilter).andThen(router());

        ListeningServer server = Http.server().serve(addr, service);

        Await.ready(server);
    }

    private static HttpMuxer router() {
        return new HttpMuxer()
                .withHandler("/cat", Handlers.echoHandler());
    }
}

