/* Copyright 2015 Twitter, Inc. */
package com.twitter.finagle.service;

/**
 * Java APIs for {@link ResponseClass}.
 */
public final class ResponseClasses {

  private ResponseClasses() {
    throw new IllegalStateException();
  }

  /**
   * See {@link ResponseClass$#Success()}
   */
  public static final ResponseClass.Successful SUCCESS =
      ResponseClass$.MODULE$.Success();

  /**
   * See {@link ResponseClass$#NonRetryableFailure()}
   */
  public static final ResponseClass.Failed NON_RETRYABLE_FAILURE =
      ResponseClass$.MODULE$.NonRetryableFailure();

  /**
   * See {@link ResponseClass$#RetryableFailure()}
   */
  public static final ResponseClass.Failed RETRYABLE_FAILURE =
      ResponseClass$.MODULE$.RetryableFailure();

  /**
   * See {@link ResponseClass$#Ignorable()}
   */
  public static final ResponseClass IGNORED =
      ResponseClass.Ignored();

}
