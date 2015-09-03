package com.twitter.finagle.httpx;

import org.junit.Test;

import com.twitter.finagle.Service;
import com.twitter.util.Function;
import com.twitter.util.Future;

/** Compilation tests for HttpMuxer */
public class HttpMuxerCompilationTest {

  /** a comment */
  @Test
  public void testHttpMuxer() {
    HttpMuxers.apply(Request.apply("http://192.168.0.1/"));

    HttpMuxers.patterns();

    HttpMuxer.addHandler(
      "com/twitter/finagle/httpx/HttpMuxerCompilationTest",
      Service.mk(new Function<Request, Future<Response>>() {
        @Override
        public Future<Response> apply(Request req) {
          return Future.value(Response.apply());
        }
      }));

  }

}
