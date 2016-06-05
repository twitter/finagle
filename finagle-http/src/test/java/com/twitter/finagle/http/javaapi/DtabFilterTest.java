package com.twitter.finagle.http.javaapi;

import org.junit.Assert;
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
    DtabFilter<Request, Response> finagleDtabFilter = new DtabFilter.Finagle<Request>();
    Assert.assertNotNull(finagleDtabFilter);
  }
}
