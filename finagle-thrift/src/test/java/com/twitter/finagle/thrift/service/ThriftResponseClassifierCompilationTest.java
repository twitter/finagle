/* Copyright 2015 Twitter, Inc. */
package com.twitter.finagle.thrift.service;

import scala.PartialFunction;

import org.junit.Test;

import com.twitter.finagle.service.ReqRep;
import com.twitter.finagle.service.ResponseClass;

public class ThriftResponseClassifierCompilationTest {

  @Test
  public void testThriftExceptionsAsFailures() {
    PartialFunction<ReqRep, ResponseClass> classifier =
        ThriftResponseClassifier.ThriftExceptionsAsFailures();
  }

}
