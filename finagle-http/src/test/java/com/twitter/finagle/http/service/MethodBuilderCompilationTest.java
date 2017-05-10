package com.twitter.finagle.http.service;

import org.junit.Test;

import com.twitter.finagle.Http;
import com.twitter.util.Duration;

public class MethodBuilderCompilationTest {

  @Test
  public void testTimeouts() {
    Http.client().methodBuilder("localhost:8080")
      .withTimeoutTotal(Duration.fromMilliseconds(100))
      .withTimeoutPerRequest(Duration.fromMilliseconds(50))
      .newService("timeouts");
  }

  @Test
  public void testRetriesDisabled() {
    Http.client().methodBuilder("localhost:8080")
      .withRetryDisabled()
      .newService("retries_disabled");
  }

  @Test
  public void testRetries() {
    Http.client().methodBuilder("localhost:8080")
      .withRetryForClassifier(HttpResponseClassifier.ServerErrorsAsFailures())
      .newService("retries");
  }

}
