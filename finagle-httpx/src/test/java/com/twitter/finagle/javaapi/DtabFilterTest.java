package com.twitter.finagle.javaapi;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.finagle.httpx.Ask;
import com.twitter.finagle.httpx.Response;
import com.twitter.finagle.httpx.filter.DtabFilter;

/**
 * A Java compilation test for DtabFilter.
 */
public class DtabFilterTest {

  @Test
  public void tesDtab() {
    DtabFilter<Ask, Response> finagleDtabFilter = new DtabFilter.Finagle<Ask>();
    Assert.assertNotNull(finagleDtabFilter);
  }
}
