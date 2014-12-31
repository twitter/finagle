package com.twitter.finagle.javaapi;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.junit.Test;

import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.finagle.http.filter.DtabFilter;

/**
 * A Java compilation test for DtabFilter.
 */
public class DtabFilterTest {

  @Test
  public void tesDtab() {
    DtabFilter<HttpRequest, HttpResponse> nettyDtabFilter = DtabFilter.Netty();
    DtabFilter<Request, Response> finagleDtabFilter = new DtabFilter.Finagle<Request>();
  }
}
