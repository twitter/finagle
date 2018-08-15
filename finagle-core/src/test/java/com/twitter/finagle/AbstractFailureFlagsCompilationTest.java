package com.twitter.finagle;

import org.junit.Assert;
import org.junit.Test;

public class AbstractFailureFlagsCompilationTest {

  private class JavaFailureFlag extends AbstractFailureFlags<JavaFailureFlag> {
    private final long fs;

    private JavaFailureFlag(long flags) {
      this.fs = flags;
    }

    public JavaFailureFlag() {
      this(JavaFailureFlags.EMPTY);
    }

    public long flags() {
      return this.fs;
    }

    public JavaFailureFlag copyWithFlags(long flags) {
      return new JavaFailureFlag(flags);
    }
  }

  @Test
  public void testBasics() {
    JavaFailureFlag empty = new JavaFailureFlag();
    Assert.assertEquals(JavaFailureFlags.EMPTY, empty.flags());

    JavaFailureFlag copied = empty.withFlags(JavaFailureFlags.RETRYABLE);
    Assert.assertEquals(JavaFailureFlags.RETRYABLE, copied.flags());
  }

}
