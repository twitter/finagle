/* Copyright 2015 Twitter, Inc. */
package com.twitter.finagle.service;

import scala.PartialFunction;
import scala.runtime.AbstractPartialFunction;

import org.junit.Test;

import static org.junit.Assert.*;

import com.twitter.finagle.Failure;
import com.twitter.util.Return;
import com.twitter.util.Throw;

public class ResponseClassifierCompilationTest {

  @Test
  public void testDefault() {
    PartialFunction<ReqRep, ResponseClass> classifier =
        ResponseClassifier.Default();

    assertEquals(
        ResponseClasses.SUCCESS,
        classifier.apply(new ReqRep(null, new Return<Object>("1"))));

    assertEquals(
        ResponseClasses.NON_RETRYABLE_FAILURE,
        classifier.apply(new ReqRep(null, new Throw<Object>(new RuntimeException()))));

    assertEquals(
        ResponseClasses.RETRYABLE_FAILURE,
        classifier.apply(new ReqRep(null, new Throw<Object>(Failure.rejected()))));
  }

  @Test
  public void testCustomResponseClassifier() {
    PartialFunction<ReqRep, ResponseClass> classifier =
        new AbstractPartialFunction<ReqRep, ResponseClass>() {
          public boolean isDefinedAt(ReqRep reqRep) {
            return reqRep.request() instanceof String;
          }

          @Override
          public ResponseClass apply(ReqRep reqRep) {
            String req = (String) reqRep.request();
            if ("ok".equals(req)) {
              return ResponseClasses.SUCCESS;
            } else {
              return ResponseClasses.NON_RETRYABLE_FAILURE;
            }
          }
        };

    assertTrue(classifier.isDefinedAt(new ReqRep("1", Return.False())));
    assertFalse(classifier.isDefinedAt(new ReqRep(5, Return.False())));

    assertEquals(
        ResponseClasses.SUCCESS,
        classifier.apply(new ReqRep("ok", Return.False())));

    assertEquals(
        ResponseClasses.NON_RETRYABLE_FAILURE,
        classifier.apply(new ReqRep("nope", Return.False())));
  }

}
