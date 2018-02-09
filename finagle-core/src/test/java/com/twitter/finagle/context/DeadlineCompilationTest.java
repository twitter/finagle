package com.twitter.finagle.context;

import scala.Option;

import org.junit.Test;

import com.twitter.util.Duration;

public class DeadlineCompilationTest {

  @Test
  public void testCurrent() {
    Option<Deadline> current = Deadline.current();
  }

  @Test
  public void ofTimeout() {
    Deadline deadline = Deadline.ofTimeout(Duration.Zero());
  }

  @Test
  public void combined() {
    Deadline deadline = Deadline.ofTimeout(Duration.Zero());
    Deadline combined = Deadline.combined(deadline, deadline);
  }

}
