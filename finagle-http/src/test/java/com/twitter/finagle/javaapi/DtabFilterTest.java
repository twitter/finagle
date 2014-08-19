package com.twitter.finagle.javaapi;

import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.finagle.http.filter.DtabFilter;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

/**
 * A Java compilation test for DtabFilter.
 */
 
public class DtabFilterTest {
  static {
    DtabFilter<HttpRequest, HttpResponse> nettyDtabFilter = DtabFilter.Netty();
    DtabFilter<Request, Response> finagleDtabFilter = new DtabFilter.Finagle<Request>();
  }
}
