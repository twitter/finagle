package com.twitter.finagle.util;

import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import com.twitter.finagle.benchmark.StdBenchAnnotations;
import com.twitter.util.Duration;
import com.twitter.util.NullTimer;

public class WindowedPercentileHistogramBenchmark extends StdBenchAnnotations {

  public static final int NUM_BUCKETS = 10;
  public static final Duration BUCKET_SIZE = Duration.fromSeconds(3);
  public static final int VALUES_PER_BUCKET = 10000;

  private static void fillBucket(WindowedPercentileHistogram wp, Random rng) {
    for (int i = 1; i <= VALUES_PER_BUCKET; i++) {
      int rand = rng.nextInt(i);
      wp.add(rand);
    }
  }

  @State(Scope.Benchmark)
  public static class WindowedPercentileState {
    // Use the Null timer so we can manually force the buckets to flush
    public final WindowedPercentileHistogram wp = new WindowedPercentileHistogram(
        NUM_BUCKETS, BUCKET_SIZE, new NullTimer());
    public final Random rng = new Random(31415926535897932L);

    /**
     * Set up the test
     */
    @Setup(Level.Trial)
    public void setup() {
      for (int i = 0; i < NUM_BUCKETS; i++) {
        fillBucket(wp, rng);
        wp.flushCurrentBucket();
      }
    }
  }

  @Benchmark
  public int percentile(WindowedPercentileState state) {
    return state.wp.percentile(0.50);
  }

  @Benchmark
  public void update(WindowedPercentileState state) {
    state.wp.flushCurrentBucket();
  }

  @Benchmark
  public void add(WindowedPercentileState state) {
    state.wp.add(state.rng.nextInt(1000));
  }
}
