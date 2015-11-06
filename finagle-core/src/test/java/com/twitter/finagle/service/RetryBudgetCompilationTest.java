/* Copyright 2015 Twitter, Inc. */
package com.twitter.finagle.service;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.util.Duration;

/**
 * Compilation unit tests for Java
 */
public class RetryBudgetCompilationTest {

  @Test
  public void testEmpty() {
    RetryBudget rb = RetryBudgets.EMPTY;
    rb.deposit();
    Assert.assertEquals(0, rb.balance());
    Assert.assertFalse(rb.tryWithdraw());
  }

  @Test
  public void testInfinite() {
    RetryBudget rb = RetryBudgets.INFINITE;
    Assert.assertTrue(rb.tryWithdraw());
  }

  @Test
  public void testNewRetryBudget() {
    RetryBudgets.newRetryBudget();
    RetryBudgets.newRetryBudget(
        Duration.fromSeconds(10),
        10,
        0.1
    );
  }

}
