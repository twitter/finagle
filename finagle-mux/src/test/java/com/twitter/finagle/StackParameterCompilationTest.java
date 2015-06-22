package com.twitter.finagle;

import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.client.StackClient;
import com.twitter.finagle.mux.exp.FailureDetector;
import com.twitter.util.Duration;

public class StackParameterCompilationTest {
  void testParams() {
    StackClient<String, String> client =
        ClientBuilder.<String, String>stackClientOfCodec(null)
          .configured(new FailureDetector.Param(FailureDetector.globalFlagConfig()).mk())
          .configured(new FailureDetector.Param(FailureDetector.nullConfig()).mk())
          .configured(
            new FailureDetector.Param(
              new FailureDetector.ThresholdConfig(Duration.forever(), 2, 5, -1)).mk());
    }
}
