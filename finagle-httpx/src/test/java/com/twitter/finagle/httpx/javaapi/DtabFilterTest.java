package com.twitter.finagle.httpx.javaapi;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.finagle.httpx.Request;
import com.twitter.finagle.httpx.Response;
import com.twitter.finagle.httpx.filter.DtabFilter;

/**
 * A Java compilation test for DtabFilter.
 */
public class DtabFilterTest {

  @Test
  public void tesDtab() {
    DtabFilter<Request, Response> finagleDtabFilter = new DtabFilter.Finagle<Request>();
    Assert.assertNotNull(finagleDtabFilter);
  }
}
