package com.twitter.finagle;

import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.client.StackClient;
import com.twitter.finagle.mux.FailureDetector;
import com.twitter.finagle.mux.FailureDetectors;
import com.twitter.util.Duration;

public class StackParameterCompilationTest {
  void testParams() {
    StackClient<String, String> client =
        ClientBuilder.<String, String>stackClientOfCodec(null)
          .configured(new FailureDetector.Param(FailureDetectors.GLOBAL_FLAG_CONFIG).mk())
          .configured(new FailureDetector.Param(FailureDetectors.NULL_CONFIG).mk())
          .configured(
            new FailureDetector.Param(
              new FailureDetector.ThresholdConfig(Duration.Top(), 2, 5, Duration.Top())).mk());
  }
}
