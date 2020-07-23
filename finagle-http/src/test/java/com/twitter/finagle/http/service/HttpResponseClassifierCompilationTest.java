/* Copyright 2015 Twitter, Inc. */
package com.twitter.finagle.http.service;

import scala.PartialFunction;
import scala.Tuple2;
import scala.runtime.AbstractPartialFunction;

import org.junit.Test;

import static org.junit.Assert.*;

import com.twitter.finagle.http.Request;
import com.twitter.finagle.http.Response;
import com.twitter.finagle.http.Status;
import com.twitter.finagle.service.ReqRep;
import com.twitter.finagle.service.ResponseClass;
import com.twitter.finagle.service.ResponseClasses;
import com.twitter.util.Return;

public class HttpResponseClassifierCompilationTest {

  private ReqRep reqRep(Status status) {
    return ReqRep.apply(
        Request.apply("/"),
        new Return<Object>(Response.apply(status))
    );
  }

  /** A test. */
  @Test
  public void testServerErrorsAsFailures() {
    PartialFunction<ReqRep, ResponseClass> classifier =
        HttpResponseClassifier.ServerErrorsAsFailures();

    assertEquals(
        ResponseClasses.NON_RETRYABLE_FAILURE,
        classifier.apply(reqRep(Status.ServiceUnavailable())));
  }

  /** A test. */
  @Test
  public void testApply() {
    PartialFunction<Tuple2<Request, Response>, ResponseClass> typed =
        new AbstractPartialFunction<Tuple2<Request, Response>, ResponseClass>() {
          @Override
          public boolean isDefinedAt(Tuple2<Request, Response> x) {
            return x._2().status() == Status.Conflict();
          }

          @Override
          public ResponseClass apply(Tuple2<Request, Response> x) {
            return ResponseClasses.RETRYABLE_FAILURE;
          }

        };

    PartialFunction<ReqRep, ResponseClass> classifier =
        HttpResponseClassifier.apply(typed);

    assertTrue(classifier.isDefinedAt(reqRep(Status.Conflict())));
    assertFalse(classifier.isDefinedAt(reqRep(Status.Ok())));

    assertEquals(
        ResponseClasses.RETRYABLE_FAILURE,
        classifier.apply(reqRep(Status.Conflict())));
  }

}
