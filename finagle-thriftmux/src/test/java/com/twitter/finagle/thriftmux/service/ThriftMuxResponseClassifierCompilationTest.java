/* Copyright 2015 Twitter, Inc. */
package com.twitter.finagle.thriftmux.service;

import scala.PartialFunction;

import org.junit.Test;

import com.twitter.finagle.service.ReqRep;
import com.twitter.finagle.service.ResponseClass;

public class ThriftMuxResponseClassifierCompilationTest {

  @Test
  public void testThriftExceptionsAsFailures() {
    PartialFunction<ReqRep, ResponseClass> classifier =
        ThriftMuxResponseClassifier.ThriftExceptionsAsFailures();
  }

}
