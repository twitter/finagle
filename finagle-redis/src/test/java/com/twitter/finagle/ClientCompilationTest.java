package com.twitter.finagle;

import org.junit.Test;

import com.twitter.finagle.param.Tracer;
import com.twitter.finagle.tracing.NullTracer;

public final class ClientCompilationTest {

  /**
   * Tests Java usage of the Redis client. The client API should be as accessible in Java as it is
   * in Scala.
   */
  @Test
  public void testClientCompilation() {
    final Redis.Client client = Redis.client().configured(new Tracer(new NullTracer()).mk());
  }
}
