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
  public void testNamedOrElse() {
    PartialFunction<ReqRep, ResponseClass> orElse =
        ResponseClassifier.Default().orElse(ResponseClassifier.Default());
    assertEquals(
        "DefaultResponseClassifier.orElse(DefaultResponseClassifier)",
        orElse.toString());
  }

  @Test
  public void testDefault() {
    PartialFunction<ReqRep, ResponseClass> classifier =
        ResponseClassifier.Default();

    assertEquals(
        ResponseClasses.SUCCESS,
        classifier.apply(ReqRep.apply(null, new Return<Object>("1"))));

    assertEquals(
        ResponseClasses.NON_RETRYABLE_FAILURE,
        classifier.apply(ReqRep.apply(null, new Throw<Object>(new RuntimeException()))));

    assertEquals(
        ResponseClasses.RETRYABLE_FAILURE,
        classifier.apply(ReqRep.apply(null, new Throw<Object>(Failure.rejected()))));

    assertEquals(
        ResponseClasses.IGNORED,
        classifier.apply(ReqRep.apply(null, new Throw<Object>(Failure.ignorable("")))));
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

    assertTrue(classifier.isDefinedAt(ReqRep.apply("1", Return.False())));
    assertFalse(classifier.isDefinedAt(ReqRep.apply(5, Return.False())));

    assertEquals(
        ResponseClasses.SUCCESS,
        classifier.apply(ReqRep.apply("ok", Return.False())));

    assertEquals(
        ResponseClasses.NON_RETRYABLE_FAILURE,
        classifier.apply(ReqRep.apply("nope", Return.False())));

    assertEquals(
        ResponseClasses.SUCCESS,
        classifier.apply(new ReqRepT<String, Object>("ok", Return.False())));

    assertEquals(
        ResponseClasses.NON_RETRYABLE_FAILURE,
        classifier.apply(new ReqRepT<String, Object>("nope", Return.False())));
  }

}
